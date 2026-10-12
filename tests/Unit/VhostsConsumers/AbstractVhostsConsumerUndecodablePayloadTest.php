<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\VhostsConsumers;

use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use RuntimeException;
use Salesmessage\LibRabbitMQ\Exceptions\UndecodablePayloadException;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\VhostsConsumers\AbstractVhostsConsumer;

class AbstractVhostsConsumerUndecodablePayloadTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const CONNECTION = 'rabbitmq_cluster:vhost:organization_1';

    private const QUEUE = 'default-quorum';

    /**
     * @return array<string, array{string, string}>
     */
    public static function undecodablePayloads(): array
    {
        return [
            'not json' => ['not json', 'Message class is not defined'],
            'no command name' => ['{"data":{}}', 'Message class is not defined'],
            'unknown class' => ['{"data":{"commandName":"App\\\\Jobs\\\\Removed"}}', 'Message class App\\Jobs\\Removed does not exist'],
            'unknown class with a NUL byte' => ['{"data":{"commandName":"App\\u0000Removed"}}', 'Message class App\\000Removed does not exist'],
            'not consumable' => ['{"data":{"commandName":"stdClass"}}', 'Job class stdClass must implement'],
        ];
    }

    #[DataProvider('undecodablePayloads')]
    public function test_undecodable_payload_is_written_to_failed_jobs_and_rejected(string $body, string $reason): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()
            ->with(self::CONNECTION, self::QUEUE, Mockery::on(
                fn (string $stored) => json_decode($stored, true)['body'] === $body
            ), Mockery::on(
                fn ($exception) => $exception instanceof UndecodablePayloadException
                    && str_contains($exception->getMessage(), $reason)
            ))
            ->andReturn(1);
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_reject')->once()->with(3, false);

        $consumer = $this->makeConsumer($failer);
        $consumer->processMessage($this->makeMessage($channel, $body), $this->makeConnection());

        $this->assertSame(0, $consumer->processedJobs());
    }

    public function test_undecodable_payload_is_requeued_when_the_row_is_not_written(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->andThrow(new RuntimeException('database is down'));
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_nack')->once()->with(3, false, true);
        $channel->shouldNotReceive('basic_reject');

        $consumer = $this->makeConsumer($failer);
        $consumer->processMessage($this->makeMessage($channel, 'not json'), $this->makeConnection());

        $this->assertSame(0, $consumer->processedJobs());
    }

    private function makeMessage(AMQPChannel $channel, string $body): AMQPMessage
    {
        $message = new AMQPMessage($body);
        $message->setChannel($channel);
        $message->setDeliveryInfo(3, false, '', self::QUEUE);

        return $message;
    }

    private function makeConnection(): RabbitMQQueue
    {
        $connection = Mockery::mock(RabbitMQQueue::class);
        $connection->shouldReceive('getJobClass')->andReturn(RabbitMQJob::class);

        return $connection;
    }

    private function makeConsumer(FailedJobProviderInterface $failer)
    {
        $deliveryLimitService = Mockery::mock(DeliveryLimitService::class);
        $deliveryLimitService->shouldReceive('isAllowed')->with(Mockery::any(), self::CONNECTION, self::QUEUE)->andReturnTrue();

        $consumer = new class(
            Mockery::mock(InternalStorageManager::class),
            new NullLogger,
            Mockery::mock(QueueManager::class),
            Mockery::mock(Dispatcher::class),
            Mockery::mock(ExceptionHandler::class),
            fn () => false,
            Mockery::mock(DeduplicationService::class),
            $deliveryLimitService,
            new FailedJobWriter($failer, new NullLogger),
        ) extends AbstractVhostsConsumer {
            protected function vhostDaemon($connectionName, WorkerOptions $options)
            {
            }

            protected function startConsuming(): ?RabbitMQQueue
            {
                return null;
            }

            protected function stopConsuming(): void
            {
            }

            public function useConnection(string $connectionName, string $queueName): void
            {
                $this->currentConnectionName = $connectionName;
                $this->currentQueueName = $queueName;
            }

            public function processMessage(AMQPMessage $message, RabbitMQQueue $connection): void
            {
                $this->processAmqpMessage($message, $connection);
            }

            public function processedJobs(): int
            {
                return $this->jobsProcessed;
            }
        };

        $consumer->setContainer(new Container);
        $consumer->useConnection(self::CONNECTION, self::QUEUE);

        return $consumer;
    }
}
