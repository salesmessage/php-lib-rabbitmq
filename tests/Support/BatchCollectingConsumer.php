<?php

namespace Salesmessage\LibRabbitMQ\Tests\Support;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Mockery;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Mutex;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\VhostsConsumers\AbstractVhostsConsumer;

class BatchCollectingConsumer extends AbstractVhostsConsumer
{
    public static function make(): self
    {
        $events = Mockery::mock(Dispatcher::class);
        $events->shouldReceive('dispatch');

        $consumer = new self(
            Mockery::mock(InternalStorageManager::class),
            Mockery::mock(LoggerInterface::class),
            Mockery::mock(QueueManager::class),
            $events,
            Mockery::mock(ExceptionHandler::class),
            fn () => false,
            Mockery::mock(DeduplicationService::class),
            Mockery::mock(DeliveryLimitService::class),
            Mockery::mock(FailedJobWriter::class),
        );
        $consumer->connectionMutex = new Mutex(false);

        return $consumer;
    }

    public function collect(AMQPMessage ...$messages): void
    {
        foreach ($messages as $message) {
            $this->addMessageToBatch($message);
        }
    }

    protected function vhostDaemon($connectionName, WorkerOptions $options) {}

    protected function startConsuming(): ?RabbitMQQueue
    {
        return null;
    }

    protected function stopConsuming(): void {}
}
