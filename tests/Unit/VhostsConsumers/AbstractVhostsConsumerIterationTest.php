<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\VhostsConsumers;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Dto\ConsumeVhostsFiltersDto;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Scheduler\VhostSchedulerInterface;
use Salesmessage\LibRabbitMQ\VhostsConsumers\AbstractVhostsConsumer;

/**
 * Vhost/queue iteration over the scheduler's ordered lists: group filters must
 * narrow the lists without truncating the pass (filter gaps used to break the
 * index-based walk in getNextVhost()/getNextQueue()).
 */
class AbstractVhostsConsumerIterationTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function testVhostIterationVisitsEveryVhostMatchingTheMask(): void
    {
        $scheduler = Mockery::mock(VhostSchedulerInterface::class);
        $scheduler->shouldReceive('getOrderedVhosts')->with('g')
            ->andReturn(['other_1', 'org_1', 'other_2', 'org_2']);
        $scheduler->shouldReceive('getOrderedQueues')->andReturn(['q1']);

        $consumer = $this->makeConsumer($scheduler, new ConsumeVhostsFiltersDto('g', [], 'org', [], ''));

        $consumer->loadVhostsPublic();

        $this->assertTrue($consumer->switchToNextVhostPublic());
        $this->assertSame('org_1', $consumer->currentVhost());
        $this->assertSame('q1', $consumer->currentQueue());

        $this->assertTrue($consumer->switchToNextVhostPublic());
        $this->assertSame('org_2', $consumer->currentVhost());

        $this->assertFalse($consumer->switchToNextVhostPublic());
    }

    public function testQueueIterationVisitsEveryQueueMatchingTheMask(): void
    {
        $scheduler = Mockery::mock(VhostSchedulerInterface::class);
        $scheduler->shouldReceive('getOrderedVhosts')->with('g')->andReturn(['org_1']);
        $scheduler->shouldReceive('getOrderedQueues')->with('g', 'org_1')
            ->andReturn(['notes_a', 'other', 'notes_b']);

        $consumer = $this->makeConsumer($scheduler, new ConsumeVhostsFiltersDto('g', [], '', [], 'notes'));

        $consumer->loadVhostsPublic();

        $this->assertTrue($consumer->switchToNextVhostPublic());
        $this->assertSame('notes_a', $consumer->currentQueue());

        $this->assertTrue($consumer->switchToNextQueuePublic());
        $this->assertSame('notes_b', $consumer->currentQueue());

        $this->assertFalse($consumer->switchToNextQueuePublic());
    }

    public function testVhostsWithNoMatchingQueuesAreSkipped(): void
    {
        $scheduler = Mockery::mock(VhostSchedulerInterface::class);
        $scheduler->shouldReceive('getOrderedVhosts')->with('g')->andReturn(['org_1', 'org_2']);
        $scheduler->shouldReceive('getOrderedQueues')->with('g', 'org_1')->andReturn(['other']);
        $scheduler->shouldReceive('getOrderedQueues')->with('g', 'org_2')->andReturn(['notes_a']);

        $consumer = $this->makeConsumer($scheduler, new ConsumeVhostsFiltersDto('g', [], '', [], 'notes'));

        $consumer->loadVhostsPublic();

        $this->assertTrue($consumer->switchToNextVhostPublic());
        $this->assertSame('org_2', $consumer->currentVhost());
        $this->assertSame('notes_a', $consumer->currentQueue());
    }

    private function makeConsumer(VhostSchedulerInterface $scheduler, ConsumeVhostsFiltersDto $filters)
    {
        $consumer = new class(
            Mockery::mock(InternalStorageManager::class),
            Mockery::mock(LoggerInterface::class),
            Mockery::mock(QueueManager::class),
            Mockery::mock(Dispatcher::class),
            Mockery::mock(ExceptionHandler::class),
            fn () => false,
            Mockery::mock(DeduplicationService::class),
            Mockery::mock(DeliveryLimitService::class),
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

            public function loadVhostsPublic(): void
            {
                $this->loadVhosts();
            }

            public function switchToNextVhostPublic(): bool
            {
                return $this->switchToNextVhost();
            }

            public function switchToNextQueuePublic(): bool
            {
                return $this->switchToNextQueue();
            }

            public function currentVhost(): ?string
            {
                return $this->currentVhostName;
            }

            public function currentQueue(): ?string
            {
                return $this->currentQueueName;
            }
        };

        $consumer->setScheduler($scheduler);
        $consumer->setFiltersDto($filters);

        return $consumer;
    }
}
