<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\VhostsConsumers;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionProperty;
use Salesmessage\LibRabbitMQ\Mutex;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\VhostsConsumers\AbstractVhostsConsumer;

/**
 * A vhost whose connection cannot be opened is removed from the index only when the failure is about the
 * vhost: during an outage (e.g. AMQProxy up, broker down) the vhosts must stay indexed for when it is back.
 */
class AbstractVhostsConsumerInitConnectionTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public static function failures(): array
    {
        return [
            'broker down behind the proxy' => [new AMQPConnectionClosedException('UPSTREAM_ERROR(0, 0)', 500), false],
            'connection lost' => [new AMQPConnectionClosedException('Broken pipe or closed connection'), false],
            'vhost not found' => [new AMQPConnectionClosedException('NOT_ALLOWED - vhost organization_1 not found(0, 0)', 403), true],
            'vhost deleted' => [new AMQPConnectionClosedException("CONNECTION_FORCED - vhost 'organization_1' is deleted", 320), true],
        ];
    }

    #[DataProvider('failures')]
    public function test_vhost_is_removed_from_the_index_only_when_the_failure_is_about_the_vhost(AMQPConnectionClosedException $failure, bool $removed): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('removeVhost')->times($removed ? 1 : 0);
        $queue = Mockery::mock(RabbitMQQueue::class);
        $queue->shouldReceive('getChannel')->with(true)->andThrow($failure);
        $manager = Mockery::mock(QueueManager::class);
        $manager->shouldReceive('connection')->andReturn($queue);

        $consumer = $this->makeConsumer($storage, $manager);

        $this->assertNull($consumer->initConnectionPublic());
    }

    private function makeConsumer(InternalStorageManager $storage, QueueManager $manager)
    {
        $consumer = new class($storage, Mockery::spy(LoggerInterface::class), $manager, Mockery::mock(Dispatcher::class), Mockery::mock(ExceptionHandler::class), fn () => false, Mockery::mock(DeduplicationService::class), Mockery::mock(DeliveryLimitService::class), Mockery::mock(FailedJobWriter::class)) extends AbstractVhostsConsumer
        {
            protected function vhostDaemon($connectionName, WorkerOptions $options) {}

            protected function startConsuming(): ?RabbitMQQueue
            {
                return null;
            }

            protected function stopConsuming(): void {}

            protected function loadVhosts(): void {}

            protected function log(string $message, array $data = [], string $logType = 'info'): void {}

            // Stop after the first failure instead of waiting for the next vhost.
            protected function goAheadOrWait(int $waitSeconds = 1): bool
            {
                return false;
            }

            public function initConnectionPublic(): ?RabbitMQQueue
            {
                return $this->initConnection();
            }
        };

        (new ReflectionProperty(AbstractVhostsConsumer::class, 'currentVhostName'))->setValue($consumer, 'organization_1');
        (new ReflectionProperty(AbstractVhostsConsumer::class, 'configConnectionName'))->setValue($consumer, 'rabbitmq_vhosts');
        (new ReflectionProperty(AbstractVhostsConsumer::class, 'connectionMutex'))->setValue($consumer, new Mutex(false));

        return $consumer;
    }
}
