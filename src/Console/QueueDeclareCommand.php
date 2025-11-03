<?php

namespace Salesmessage\LibRabbitMQ\Console;

use Exception;
use Illuminate\Console\Command;
use Salesmessage\LibRabbitMQ\Queue\Connectors\RabbitMQConnector;

class QueueDeclareCommand extends Command
{
    protected $signature = 'lib-rabbitmq:queue-declare
                           {name : The name of the queue to declare}
                           {connection=rabbitmq : The name of the queue connection to use}
                           {--max-priority : Set x-max-priority (ignored for quorum)}
                           {--durable=1}
                           {--auto-delete=0}
                           {--quorum=0 : Declare quorum queue (x-queue-type=quorum)}
                           {--quorum-initial-group-size= : x-quorum-initial-group-size when quorum is enabled}';

    protected $description = 'Declare queue';

    /**
     * @throws Exception
     */
    public function handle(RabbitMQConnector $connector): void
    {
        $config = $this->laravel['config']->get('queue.connections.'.$this->argument('connection'));

        $queue = $connector->connect($config);

        if ($queue->isQueueExists($this->argument('name'))) {
            $this->warn('Queue already exists.');

            return;
        }

        $arguments = [];

        $maxPriority = (int) $this->option('max-priority');
        $isQuorum = (bool) $this->option('quorum');

        if ($isQuorum) {
            $arguments['x-queue-type'] = 'quorum';

            $initialGroupSize = (int) $this->option('quorum-initial-group-size');
            if ($initialGroupSize > 0) {
                $arguments['x-quorum-initial-group-size'] = $initialGroupSize;
            }

            if ($maxPriority) {
                // quorum queues do not support priority; ignore and warn
                $this->warn('Ignoring --max-priority for quorum queue.');
            }
        } else {
            if ($maxPriority) {
                $arguments['x-max-priority'] = $maxPriority;
            }
        }

        $queue->declareQueue(
            $this->argument('name'),
            (bool) $this->option('durable'),
            (bool) $this->option('auto-delete'),
            $arguments
        );

        $this->info('Queue declared successfully.');
    }
}
