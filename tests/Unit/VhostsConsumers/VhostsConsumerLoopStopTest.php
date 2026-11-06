<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\VhostsConsumers;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Dto\ConsumeVhostsFiltersDto;
use Salesmessage\LibRabbitMQ\Mutex;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Scheduler\VhostSchedulerInterface;
use Salesmessage\LibRabbitMQ\Tests\Mocks\TestBatchableJob;
use Salesmessage\LibRabbitMQ\Tests\Support\RecordsProcessedBatches;
use Salesmessage\LibRabbitMQ\VhostsConsumers\AbstractVhostsConsumer;
use Salesmessage\LibRabbitMQ\VhostsConsumers\DirectConsumer;
use Salesmessage\LibRabbitMQ\VhostsConsumers\QueueConsumer;

class VhostsConsumerLoopStopTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const VHOST = 'organization_1';

    private const COLLECTED_MESSAGES = 2;

    /** @var list<string> */
    private array $channelCalls = [];

    public static function stops(): array
    {
        return [
            'SIGTERM while collecting' => [10, 2, 'sigterm', true],
            '--max-time while collecting' => [10, 2, 'max-time', true],
            'SIGTERM before a drained queue is processed' => [10, 3, 'sigterm', true],
            'SIGTERM before a full batch is processed' => [2, 2, 'sigterm', true],
            '--max-time before a drained queue is processed' => [10, 3, 'max-time', false],
            '--max-time before a full batch is processed' => [2, 2, 'max-time', false],
            '--max-jobs equal to the batch size' => [2, 1, 'max-jobs', false],
        ];
    }

    #[DataProvider('stops')]
    public function test_direct_consumer_returns_or_processes_the_collected_batch_on_stop(int $batchSize, int $triggerAt, string $trigger, bool $returned): void
    {
        $options = new WorkerOptions(memory: 1024, sleep: 0);
        $channel = $this->channel();
        $consumer = $this->consumer(DirectConsumer::class, $channel, $batchSize, $options);

        $gets = 0;
        $channel->shouldReceive('basic_get')->andReturnUsing(
            function () use (&$gets, $triggerAt, $trigger, $consumer, $options, $channel): ?AMQPMessage {
                $gets++;
                if ($gets === $triggerAt) {
                    $this->trigger($trigger, $consumer, $options);
                }

                return $gets <= self::COLLECTED_MESSAGES ? $this->message($channel, $gets) : null;
            }
        );

        $this->assertSame(0, $this->runDaemon($consumer, $options));
        $this->assertOutcome($returned, $consumer);
    }

    #[DataProvider('stops')]
    public function test_queue_consumer_returns_or_processes_the_collected_batch_on_stop(int $batchSize, int $triggerAt, string $trigger, bool $returned): void
    {
        $options = new WorkerOptions(memory: 1024, sleep: 0);
        $channel = $this->channel();
        $consumer = $this->consumer(QueueConsumer::class, $channel, $batchSize, $options);

        $callback = null;
        $channel->shouldReceive('basic_consume')->andReturnUsing(function (...$arguments) use (&$callback): string {
            $callback = $arguments[6];

            return $arguments[1];
        });
        $channel->shouldReceive('is_consuming')->andReturnTrue();
        $waits = 0;
        $channel->shouldReceive('wait')->andReturnUsing(
            function () use (&$waits, &$callback, $triggerAt, $trigger, $consumer, $options, $channel): void {
                $waits++;
                if ($waits === $triggerAt) {
                    $this->trigger($trigger, $consumer, $options);
                }
                if ($waits <= self::COLLECTED_MESSAGES) {
                    $callback($this->message($channel, $waits));
                }
            }
        );

        $this->assertSame(0, $this->runDaemon($consumer, $options));
        $this->assertOutcome($returned, $consumer);

        if ($returned) {
            $nack = array_search('nack 2 multiple requeue', $this->channelCalls, true);
            $cancel = array_search('cancel tag_'.self::VHOST.' nowait', $this->channelCalls, true);
            $this->assertNotFalse($cancel);
            $this->assertLessThan($nack, $cancel);
        }
    }

    private function assertOutcome(bool $returned, AbstractVhostsConsumer $consumer): void
    {
        $nacks = array_values(preg_grep('/^nack/', $this->channelCalls));
        if ($returned) {
            $this->assertSame(['nack 2 multiple requeue'], $nacks);
            $this->assertSame([], $consumer->processedBatches);

            return;
        }

        $this->assertSame([], $nacks);
        $this->assertSame([[1, 2]], $consumer->processedBatches);
    }

    private function trigger(string $trigger, AbstractVhostsConsumer $consumer, WorkerOptions $options): void
    {
        match ($trigger) {
            'sigterm' => $consumer->shouldQuit = true,
            'max-jobs' => $options->maxJobs = self::COLLECTED_MESSAGES,
            'max-time' => $this->exceedMaxTime($consumer, $options),
        };
    }

    private function exceedMaxTime(AbstractVhostsConsumer $consumer, WorkerOptions $options): void
    {
        $options->maxTime = 1;
        $this->setProperty($consumer, 'startTime', hrtime(true) / 1e9 - 10);
    }

    private function channel(): AMQPChannel
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_qos');
        $channel->shouldReceive('basic_cancel')->andReturnUsing(function (string $tag, bool $nowait = false): string {
            $this->channelCalls[] = 'cancel '.$tag.($nowait ? ' nowait' : '');

            return $tag;
        });
        $channel->shouldReceive('basic_nack')->andReturnUsing(function (int $tag, bool $multiple, bool $requeue): void {
            $this->channelCalls[] = 'nack '.$tag.($multiple ? ' multiple' : '').($requeue ? ' requeue' : '');
        });

        return $channel;
    }

    /**
     * @param  class-string<AbstractVhostsConsumer>  $class
     */
    private function consumer(string $class, AMQPChannel $channel, int $batchSize, WorkerOptions $options): AbstractVhostsConsumer
    {
        $queue = Mockery::mock(RabbitMQQueue::class);
        $queue->shouldReceive('getChannel')->with(true)->andReturn($channel);
        $queue->shouldReceive('getConnectionName')->andReturn('rabbitmq_vhosts:'.self::VHOST);
        $queue->shouldNotReceive('getJobClass');
        $manager = Mockery::mock(QueueManager::class);
        $manager->shouldReceive('connection')->andReturn($queue);
        $deliveryLimit = Mockery::mock(DeliveryLimitService::class);
        $deliveryLimit->shouldReceive('isAllowed')->andReturnTrue();
        $events = Mockery::mock(Dispatcher::class);
        $events->shouldReceive('dispatch');
        $scheduler = Mockery::mock(VhostSchedulerInterface::class);
        $scheduler->shouldReceive('reserve');

        $arguments = [
            Mockery::mock(InternalStorageManager::class),
            Mockery::mock(LoggerInterface::class)->shouldIgnoreMissing(),
            $manager,
            $events,
            Mockery::mock(ExceptionHandler::class),
            fn () => false,
            Mockery::mock(DeduplicationService::class),
            $deliveryLimit,
            Mockery::mock(FailedJobWriter::class),
        ];
        $consumer = $class === DirectConsumer::class
            ? new class(...$arguments) extends DirectConsumer
            {
                use RecordsProcessedBatches;
            }
        : new class(...$arguments) extends QueueConsumer
        {
            use RecordsProcessedBatches;
        };
        $consumer->setFiltersDto(new ConsumeVhostsFiltersDto('g', [], '', [], ''));
        $consumer->setScheduler($scheduler);
        $consumer->setBatchSize($batchSize);
        $consumer->setConsumerTag('tag');

        $this->setProperty($consumer, 'connectionMutex', new Mutex(false));
        $this->setProperty($consumer, 'workerOptions', $options);
        $this->setProperty($consumer, 'startTime', hrtime(true) / 1e9);
        $this->setProperty($consumer, 'configConnectionName', 'rabbitmq_vhosts');
        $this->setProperty($consumer, 'currentVhostName', self::VHOST);
        $this->setProperty($consumer, 'currentQueueName', 'queue');

        return $consumer;
    }

    private function runDaemon(AbstractVhostsConsumer $consumer, WorkerOptions $options): ?int
    {
        return (new \ReflectionMethod($consumer, 'vhostDaemon'))->invoke($consumer, 'rabbitmq_vhosts', $options);
    }

    private function message(AMQPChannel $channel, int $deliveryTag): AMQPMessage
    {
        $message = new AMQPMessage(json_encode(['data' => ['commandName' => TestBatchableJob::class]]));
        $message->setChannel($channel);
        $message->setDeliveryInfo($deliveryTag, false, '', 'queue');

        return $message;
    }

    private function setProperty(object $object, string $property, mixed $value): void
    {
        (new \ReflectionProperty($object, $property))->setValue($object, $value);
    }
}
