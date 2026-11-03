<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\VhostsConsumers;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionProperty;
use Salesmessage\LibRabbitMQ\Dto\ConsumeVhostsFiltersDto;
use Salesmessage\LibRabbitMQ\Mutex;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Scheduler\VhostSchedulerInterface;
use Salesmessage\LibRabbitMQ\VhostsConsumers\AbstractVhostsConsumer;

/**
 * A vhost whose connection cannot be opened is taken out of the index, so the consumers go on with the next
 * vhost; only when the whole broker is out of reach (AMQProxy up, broker down) do the vhosts stay indexed.
 */
class AbstractVhostsConsumerInitConnectionTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public static function failures(): array
    {
        return [
            'broker down behind the proxy' => [new AMQPConnectionClosedException('UPSTREAM_ERROR - Connection refused(0, 0)', 503), false],
            'connection lost' => [new AMQPConnectionClosedException('Broken pipe or closed connection'), true],
            'vhost down while its node restarts' => [new AMQPConnectionClosedException("NOT_ALLOWED - access to vhost 'organization_1' refused for user 'u': vhost 'organization_1' is down", 530), true],
            'vhost connection limit' => [new AMQPConnectionClosedException("NOT_ALLOWED - access to vhost 'organization_1' refused for user 'u': connection limit (100) is reached", 530), true],
            'vhost not found' => [new AMQPConnectionClosedException('NOT_ALLOWED - vhost organization_1 not found(0, 0)', 403), true],
            'vhost deleted' => [new AMQPConnectionClosedException("CONNECTION_FORCED - vhost 'organization_1' is deleted", 320), true],
        ];
    }

    #[DataProvider('failures')]
    public function test_failing_vhost_is_removed_from_the_index_unless_the_whole_broker_is_unreachable(AMQPConnectionClosedException $failure, bool $removed): void
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

    public function test_consumer_waits_for_an_unreachable_broker_without_giving_up_or_dropping_vhosts(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldNotReceive('removeVhost');
        $failures = 15;
        $queue = Mockery::mock(RabbitMQQueue::class);
        $queue->shouldReceive('getChannel')->with(true)->andReturnUsing(function () use (&$failures) {
            if ($failures-- > 0) {
                throw new AMQPConnectionClosedException('UPSTREAM_ERROR - Connection refused(0, 0)', 503);
            }

            return Mockery::mock(AMQPChannel::class)->shouldIgnoreMissing();
        });
        $queue->shouldReceive('getConnectionName')->andReturn('rabbitmq_vhosts:organization_1');
        $manager = Mockery::mock(QueueManager::class);
        $manager->shouldReceive('connection')->andReturn($queue);
        $scheduler = Mockery::mock(VhostSchedulerInterface::class);
        $scheduler->shouldReceive('getOrderedVhosts')->andReturn(['organization_1']);
        $scheduler->shouldReceive('getOrderedQueues')->andReturn(['q']);

        $consumer = $this->makeWaitingConsumer($storage, $manager, $scheduler);

        $this->assertSame($queue, $consumer->initConnectionPublic());
        $this->assertSame(-1, $failures);
    }

    public function test_consumer_gives_up_after_too_many_failed_connection_attempts(): void
    {
        // A failure other than an unreachable broker counts toward the limit, as on 1.83.
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('removeVhost');
        $queue = Mockery::mock(RabbitMQQueue::class);
        $queue->shouldReceive('getChannel')->with(true)->andThrow(new AMQPConnectionClosedException('Broken pipe or closed connection'));
        $manager = Mockery::mock(QueueManager::class);
        $manager->shouldReceive('connection')->andReturn($queue);
        $scheduler = Mockery::mock(VhostSchedulerInterface::class);
        $scheduler->shouldReceive('getOrderedVhosts')->andReturn(['organization_1']);
        $scheduler->shouldReceive('getOrderedQueues')->andReturn(['q']);

        $consumer = $this->makeWaitingConsumer($storage, $manager, $scheduler);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Too many connection attempts');
        $consumer->initConnectionPublic();
    }

    public function test_vhost_that_stays_down_does_not_block_the_next_one(): void
    {
        // Ordered by last processed: organization_1 never gets processed, so it would stay first for every consumer.
        $scheduler = new class implements VhostSchedulerInterface
        {
            public array $vhosts = ['organization_1', 'organization_2'];

            public function getOrderedVhosts(string $group): array
            {
                return $this->vhosts;
            }

            public function getOrderedQueues(string $group, string $vhost): array
            {
                return ['q'];
            }

            public function reserve(string $group, string $vhost, string $queue): void {}

            public function record(string $group, string $vhost, string $queue, int $milliseconds): void {}

            public function accrue(string $group, string $vhost, int $elapsedMs): void {}
        };
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('removeVhost')->andReturnUsing(function ($dto) use ($scheduler): void {
            $scheduler->vhosts = array_values(array_diff($scheduler->vhosts, [$dto->getName()]));
        });
        $down = new AMQPConnectionClosedException("NOT_ALLOWED - access to vhost 'organization_1' refused for user 'u': vhost 'organization_1' is down(0, 0)", 403);
        $bad = Mockery::mock(RabbitMQQueue::class);
        $bad->shouldReceive('getChannel')->with(true)->andThrow($down);
        $good = Mockery::mock(RabbitMQQueue::class);
        $good->shouldReceive('getChannel')->with(true)->andReturn(Mockery::mock(AMQPChannel::class)->shouldIgnoreMissing());
        $good->shouldReceive('getConnectionName')->andReturn('rabbitmq_vhosts:organization_2');
        $attempts = ['organization_1' => 0, 'organization_2' => 0];
        $manager = Mockery::mock(QueueManager::class);
        $manager->shouldReceive('connection')->andReturnUsing(function (string $name) use ($bad, $good, &$attempts) {
            $vhost = str_contains($name, 'organization_1') ? 'organization_1' : 'organization_2';
            $attempts[$vhost]++;

            return $vhost === 'organization_1' ? $bad : $good;
        });

        $consumer = $this->makeWaitingConsumer($storage, $manager, $scheduler);

        $this->assertSame($good, $consumer->initConnectionPublic());
        $this->assertSame('organization_2', $consumer->current());
        $this->assertSame(1, $attempts['organization_1']);
    }

    /** A consumer that runs its real vhost iteration, without stopping or sleeping. */
    private function makeWaitingConsumer(InternalStorageManager $storage, QueueManager $manager, VhostSchedulerInterface $scheduler)
    {
        $consumer = new class($storage, Mockery::spy(LoggerInterface::class), $manager, Mockery::mock(Dispatcher::class), Mockery::mock(ExceptionHandler::class), fn () => false, Mockery::mock(DeduplicationService::class), Mockery::mock(DeliveryLimitService::class), Mockery::mock(FailedJobWriter::class)) extends AbstractVhostsConsumer
        {
            protected function vhostDaemon($connectionName, WorkerOptions $options) {}

            protected function startConsuming(): ?RabbitMQQueue
            {
                return null;
            }

            protected function stopConsuming(): void {}

            protected function log(string $message, array $data = [], string $logType = 'info'): void {}

            protected function stopIfNecessary(WorkerOptions $options, $lastRestart, $startTime = 0, $jobsProcessed = 0, $job = null, $lastJobProcessedAt = null)
            {
                return null;
            }

            public function sleep($seconds) {}

            public function initConnectionPublic(): ?RabbitMQQueue
            {
                return $this->initConnection();
            }

            public function current(): ?string
            {
                return $this->currentVhostName;
            }
        };
        $consumer->setScheduler($scheduler);
        $consumer->setFiltersDto(new ConsumeVhostsFiltersDto('g', [], '', [], ''));
        (new ReflectionProperty(AbstractVhostsConsumer::class, 'currentVhostName'))->setValue($consumer, 'organization_1');
        (new ReflectionProperty(AbstractVhostsConsumer::class, 'currentQueueName'))->setValue($consumer, 'q');
        (new ReflectionProperty(AbstractVhostsConsumer::class, 'configConnectionName'))->setValue($consumer, 'rabbitmq_vhosts');
        (new ReflectionProperty(AbstractVhostsConsumer::class, 'connectionMutex'))->setValue($consumer, new Mutex(false));
        (new ReflectionProperty(AbstractVhostsConsumer::class, 'workerOptions'))->setValue($consumer, new WorkerOptions(sleep: 0));

        return $consumer;
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
