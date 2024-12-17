<?php

namespace Salesmessage\LibRabbitMQ\Queue\Connectors;

use Exception;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\WorkerStopping;
use Salesmessage\LibRabbitMQ\Horizon\Listeners\RabbitMQFailedEvent;
use Salesmessage\LibRabbitMQ\Horizon\RabbitMQQueue as HorizonRabbitMQQueue;
use Salesmessage\LibRabbitMQ\Queue\Connection\ConnectionFactory;
use Salesmessage\LibRabbitMQ\Queue\QueueFactory;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;

class RabbitMQVhostsConnector implements ConnectorInterface
{
    protected Dispatcher $dispatcher;

    public function __construct(Dispatcher $dispatcher)
    {
        $this->dispatcher = $dispatcher;
    }

    /**
     * Establish a queue connection.
     *
     * @return RabbitMQQueue
     *
     * @throws Exception
     */
    public function connect(array $config): Queue
    {
        $connection = ConnectionFactory::make($config);

        $queue = QueueFactory::make($config)->setConnection($connection);

        if ($queue instanceof HorizonRabbitMQQueue) {
            $this->dispatcher->listen(JobFailed::class, RabbitMQFailedEvent::class);
        }

        $this->dispatcher->listen(WorkerStopping::class, static function () use ($queue): void {
            $queue->close();
        });

        return $queue;
    }
}
