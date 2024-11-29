<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\QueueManager as RabbitMQQueueManager;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueueBatchable;

class VhostsConsumer extends Consumer
{
    private ?OutputStyle $output = null;

    private string $configConnectionName = '';

    private string $currentConnectionName = '';

    private string $currentVhostName = '/';

    private ?string $currentQueueName = null;

    private ?WorkerOptions $workerOptions = null;


    public function setOutput(OutputStyle $output)
    {
        $this->output = $output;
    }

    public function daemon($connectionName, $queue, WorkerOptions $options)
    {
        $this->configConnectionName = (string) $connectionName;
        $this->currentQueueName = $queue;
        $this->workerOptions = $options;

        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        $lastRestart = $this->getTimestampOfLastQueueRestart();

        [$startTime, $jobsProcessed] = [hrtime(true) / 1e9, 0];

        /** @var RabbitMQQueue $connection */
        $connection = $this->manager->rabbitConnectionByVhost('/', $this->configConnectionName);
        $this->currentConnectionName = $connection->getConnectionName();

        $this->channel = $connection->getChannel();
        $this->connectionMutex = new Mutex(false);

        $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
        $this->channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            false
        );
        $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);

        $this->startConsuming();

        while ($this->channel->is_consuming()) {
            // Before reserving any jobs, we will make sure this queue is not paused and
            // if it is we will just pause this worker for a given amount of time and
            // make sure we do not need to kill this worker process off completely.
            if (! $this->daemonShouldRun($this->workerOptions, $this->configConnectionName, $this->currentQueueName)) {
                $this->output->info(['Consuming pause worker...', $this->currentQueueName]);

                $this->pauseWorker($this->workerOptions, $lastRestart);

                continue;
            }

            // If the daemon should run (not in maintenance mode, etc.), then we can wait for a job.
            try {
                $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
                $this->channel->wait(null, true, (int) $this->workerOptions->timeout);
                $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
            } catch (AMQPRuntimeException $exception) {
                $this->output->info(['Consuming AMQP Runtime exception...', $exception->getMessage()]);

                $this->exceptions->report($exception);

                $this->kill(self::EXIT_ERROR, $this->workerOptions);
            } catch (Exception|Throwable $exception) {
                $this->output->info(['Consuming exception...', $exception->getMessage()]);

                $this->exceptions->report($exception);

                $this->stopWorkerIfLostConnection($exception);
            }

            // If no job is got off the queue, we will need to sleep the worker.
            if ($this->currentJob === null) {
                $this->output->info(['Consuming sleep. No job...', $this->workerOptions->sleep]);

                $this->switchToNextQueue();

                $this->sleep($this->workerOptions->sleep);
            }

            // Finally, we will check to see if we have exceeded our memory limits or if
            // the queue should restart based on other indications. If so, we'll stop
            // this worker and let whatever is "monitoring" it restart the process.
            $status = $this->stopIfNecessary(
                $this->workerOptions,
                $lastRestart,
                $startTime,
                $jobsProcessed,
                $this->currentJob
            );

            if (! is_null($status)) {
                $this->output->info(['Consuming stop.', $status]);

                return $this->stop($status, $this->workerOptions);
            }

            $this->currentJob = null;
        }
    }

    private function startConsuming()
    {
        $this->output->info(['Start consuming...', $this->currentVhostName, $this->currentQueueName]);

        $arguments = [];
        if ($this->maxPriority) {
            $arguments['priority'] = ['I', $this->maxPriority];
        }

        $jobsProcessed = 0;

        /** @var RabbitMQQueue $connection */
        $connection = $this->manager->rabbitConnectionByVhost($this->currentVhostName, $this->configConnectionName);
        $this->currentConnectionName = $connection->getConnectionName();
        $this->channel = $connection->getChannel();

        $jobClass = $connection->getJobClass();

        $callback = function (AMQPMessage $message) use ($connection, $jobClass, &$jobsProcessed): void {
            $job = new $jobClass(
                $this->container,
                $connection,
                $message,
                $this->currentConnectionName,
                $this->currentQueueName
            );

            $this->output->info(['Consume message...', $this->currentQueueName, $jobsProcessed]);

            $this->currentJob = $job;

            if ($this->supportsAsyncSignals()) {
                $this->registerTimeoutHandler($job, $this->workerOptions);
            }

            $jobsProcessed++;

            $this->runJob($job, $this->currentConnectionName, $this->workerOptions);

            if ($this->supportsAsyncSignals()) {
                $this->resetTimeoutHandler();
            }

            if ($jobsProcessed >= 5) {
                $this->switchToNextQueue();
            }

            if ($this->workerOptions->rest > 0) {
                $this->sleep($this->workerOptions->rest);
            }
        };

        $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
        $this->channel->basic_consume(
            $this->currentQueueName,
            $this->consumerTag,
            false,
            false,
            false,
            false,
            $callback,
            null,
            $arguments
        );
        $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
    }

    private function stopConsuming()
    {
        $this->output->info(['Stop consuming...', $this->currentVhostName, $this->currentQueueName]);

        $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
        $this->channel->basic_cancel($this->consumerTag, true);
        $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
    }

    private function setNextQueue(): void
    {
        if ('local-vshcherbyna.notes.666' === $this->currentQueueName) {
            $this->currentQueueName = 'local-vshcherbyna.notes.777';
            $this->currentVhostName = '/';

            return;
        }

        if ('local-vshcherbyna.notes.777' === $this->currentQueueName) {
            $this->currentQueueName = 'local-vshcherbyna.notes.333';
            $this->currentVhostName = 'foo';

            return;
        }

        if ('local-vshcherbyna.notes.333' === $this->currentQueueName) {
            $this->currentQueueName = 'local-vshcherbyna.notes.555';
            $this->currentVhostName = 'foo';

            return;
        }

        if ('local-vshcherbyna.notes.555' === $this->currentQueueName) {
            $this->currentQueueName = 'local-vshcherbyna.notes.666';
            $this->currentVhostName = '/';

            return;
        }
    }

    private function switchToNextQueue()
    {
        $this->stopConsuming();

        $this->setNextQueue();

        $this->startConsuming();
    }
}

