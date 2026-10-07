<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\VhostsConsumers;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Dto\ConsumeVhostsFiltersDto;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Scheduler\VhostSchedulerInterface;
use Salesmessage\LibRabbitMQ\VhostsConsumers\AbstractVhostsConsumer;

class AbstractVhostsConsumerQueueNotFoundTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_only_a_404_channel_error_means_the_queue_is_gone(): void
    {
        $consumer = $this->makeConsumer(Mockery::mock(InternalStorageManager::class));

        $this->assertTrue($consumer->isQueueNotFoundPublic(new AMQPProtocolChannelException(404, 'NOT_FOUND', [60, 70])));
        $this->assertFalse($consumer->isQueueNotFoundPublic(new AMQPProtocolChannelException(406, 'PRECONDITION_FAILED', [60, 70])));
        $this->assertFalse($consumer->isQueueNotFoundPublic(new AMQPChannelClosedException('closed')));
    }

    public function test_a_gone_queue_is_removed_from_the_index(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('removeQueue')->once()->with(Mockery::on(
            fn (QueueApiDto $dto) => ($dto->getVhostName() === 'org_1') && ($dto->getName() === 'q1')
        ));

        $consumer = $this->makeConsumer($storage);
        $consumer->loadVhostsPublic();
        $consumer->switchToNextVhostPublic();

        $consumer->forgetCurrentQueuePublic();
    }

    public function test_an_index_failure_does_not_stop_the_consumer(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('removeQueue')->once()->andThrow(new \RuntimeException('redis is down'));

        $consumer = $this->makeConsumer($storage);
        $consumer->loadVhostsPublic();
        $consumer->switchToNextVhostPublic();

        $consumer->forgetCurrentQueuePublic();

        $this->addToAssertionCount(1);
    }

    private function makeConsumer(InternalStorageManager $storage)
    {
        $scheduler = Mockery::mock(VhostSchedulerInterface::class);
        $scheduler->shouldReceive('getOrderedVhosts')->andReturn(['org_1']);
        $scheduler->shouldReceive('getOrderedQueues')->andReturn(['q1']);

        $consumer = new class($storage, Mockery::mock(LoggerInterface::class), Mockery::mock(QueueManager::class), Mockery::mock(Dispatcher::class), Mockery::mock(ExceptionHandler::class), fn () => false, Mockery::mock(DeduplicationService::class), Mockery::mock(DeliveryLimitService::class)) extends AbstractVhostsConsumer
        {
            protected function vhostDaemon($connectionName, WorkerOptions $options) {}

            protected function startConsuming(): ?RabbitMQQueue
            {
                return null;
            }

            protected function stopConsuming(): void {}

            public function loadVhostsPublic(): void
            {
                $this->loadVhosts();
            }

            public function switchToNextVhostPublic(): bool
            {
                return $this->switchToNextVhost();
            }

            public function isQueueNotFoundPublic(\Throwable $exception): bool
            {
                return $this->isQueueNotFound($exception);
            }

            public function forgetCurrentQueuePublic(): void
            {
                $this->forgetCurrentQueue();
            }
        };

        $consumer->setScheduler($scheduler);
        $consumer->setFiltersDto(new ConsumeVhostsFiltersDto('g', [], '', [], ''));

        return $consumer;
    }
}
