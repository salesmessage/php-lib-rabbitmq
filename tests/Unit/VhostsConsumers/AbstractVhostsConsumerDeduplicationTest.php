<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\VhostsConsumers;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\NullLogger;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Tests\TestCase;
use Salesmessage\LibRabbitMQ\VhostsConsumers\AbstractVhostsConsumer;

class AbstractVhostsConsumerDeduplicationTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const QUEUE = 'default-quorum';

    protected function setUp(): void
    {
        parent::setUp();

        config()->set('queue.drivers.rabbitmq_vhosts.deduplication.application.enabled', false);
    }

    public function test_processed_message_is_marked_as_processed(): void
    {
        $message = new AMQPMessage('{}');
        $deduplicationService = $this->makeDeduplicationService();
        $deduplicationService->shouldReceive('markAsProcessed')->once()->with($message, self::QUEUE);
        $deduplicationService->shouldNotReceive('release');

        $this->makeConsumer($deduplicationService)->processJob($this->makeJob(false), $message);
    }

    public function test_message_requeued_after_fail_releases_its_deduplication_key(): void
    {
        $message = new AMQPMessage('{}');
        $deduplicationService = $this->makeDeduplicationService();
        $deduplicationService->shouldReceive('release')->once()->with($message, self::QUEUE);
        $deduplicationService->shouldNotReceive('markAsProcessed');

        $this->makeConsumer($deduplicationService)->processJob($this->makeJob(true), $message);
    }

    private function makeDeduplicationService(): DeduplicationService
    {
        $deduplicationService = Mockery::mock(DeduplicationService::class);
        $deduplicationService->shouldReceive('decorateWithDeduplication')->once()
            ->andReturnUsing(function (\Closure $handler) {
                $handler();

                return true;
            });

        return $deduplicationService;
    }

    private function makeJob(bool $isRequeuedAfterFail): RabbitMQJob
    {
        $job = Mockery::mock(RabbitMQJob::class);
        $job->shouldReceive('isRequeuedAfterFail')->andReturn($isRequeuedAfterFail);

        return $job;
    }

    private function makeConsumer(DeduplicationService $deduplicationService)
    {
        $consumer = new class(Mockery::mock(InternalStorageManager::class), new NullLogger, Mockery::mock(QueueManager::class), Mockery::mock(Dispatcher::class), Mockery::mock(ExceptionHandler::class), fn () => false, $deduplicationService, Mockery::mock(DeliveryLimitService::class), Mockery::mock(FailedJobWriter::class)) extends AbstractVhostsConsumer
        {
            protected function vhostDaemon($connectionName, WorkerOptions $options) {}

            protected function startConsuming(): ?RabbitMQQueue
            {
                return null;
            }

            protected function stopConsuming(): void {}

            protected function supportsAsyncSignals()
            {
                return false;
            }

            protected function runJob($job, $connectionName, WorkerOptions $options) {}

            public function processJob(RabbitMQJob $job, AMQPMessage $message): void
            {
                $this->currentQueueName = 'default-quorum';
                $this->workerOptions = new WorkerOptions;
                $this->processSingleJob($job, $message);
            }
        };

        return $consumer;
    }
}
