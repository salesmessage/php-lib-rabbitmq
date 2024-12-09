<?php

namespace Salesmessage\LibRabbitMQ\Console;

use Exception;
use Illuminate\Console\Command;
use Salesmessage\LibRabbitMQ\Queue\Connectors\RabbitMQConnector;

class QueueBindCommand extends Command
{
    protected $signature = 'lib-rabbitmq:queue-bind                          
                           {queue}
                           {exchange}
                           {connection=rabbitmq : The name of the queue connection to use}
                           {--routing-key= : Bind queue to exchange via routing key}';

    protected $description = 'Bind queue to exchange';

    /**
     * @throws Exception
     */
    public function handle(RabbitMQConnector $connector): void
    {
        $config = $this->laravel['config']->get('queue.connections.'.$this->argument('connection'));

        $queue = $connector->connect($config);

        $queue->bindQueue(
            $this->argument('queue'),
            $this->argument('exchange'),
            (string) $this->option('routing-key')
        );

        $this->info('Queue bound to exchange successfully.');
    }
}
