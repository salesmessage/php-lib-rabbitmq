<?php

namespace Salesmessage\LibRabbitMQ\VhostsConsumers;

use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Support\Str;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;

class QueueConsumer extends AbstractVhostsConsumer
{
    protected bool $hasJob = false;

    protected function vhostDaemon($connectionName, WorkerOptions $options)
    {
        $lastRestart = $this->getTimestampOfLastQueueRestart();

        [$startTime, $jobsProcessed] = [hrtime(true) / 1e9, 0];

        $connection = $this->startConsuming();

        while ($this->channel->is_consuming()) {
            // Before reserving any jobs, we will make sure this queue is not paused and
            // if it is we will just pause this worker for a given amount of time and
            // make sure we do not need to kill this worker process off completely.
            if (! $this->daemonShouldRun($this->workerOptions, $this->configConnectionName, $this->currentQueueName)) {
                $this->output->info('Consuming pause worker...');

                $this->pauseWorker($this->workerOptions, $lastRestart);

                continue;
            }

            // If the daemon should run (not in maintenance mode, etc.), then we can wait for a job.
            try {
                $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
                $this->channel->wait(null, true, (int) $this->workerOptions->timeout);
                $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);
            } catch (AMQPRuntimeException $exception) {
                $this->logError('daemon.amqp_runtime_exception', [
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                ]);

                $this->exceptions->report($exception);

                $this->kill(self::EXIT_SUCCESS, $this->workerOptions);
            } catch (Exception|Throwable $exception) {
                $this->logError('daemon.exception', [
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                    'error_class' => get_class($exception),
                ]);

                $this->exceptions->report($exception);

                $this->stopWorkerIfLostConnection($exception);
            }

            // If no job is got off the queue, we will need to sleep the worker.
            if (false === $this->hasJob) {
                $this->output->info('Consuming sleep. No job...');

                $this->stopConsuming();

                $this->processBatch($connection);

                $this->goAheadOrWait();
                $this->startConsuming();

                $this->sleep($this->workerOptions->sleep);
            }

            // Finally, we will check to see if we have exceeded our memory limits or if
            // the queue should restart based on other indications. If so, we'll stop
            // this worker and let whatever is "monitoring" it restart the process.
            $this->stopStatusCode = $this->getStopStatus(
                $this->workerOptions,
                $lastRestart,
                $startTime,
                $jobsProcessed,
                $this->hasJob
            );
            if (! is_null($this->stopStatusCode)) {
                $this->logInfo('consuming_stop', [
                    'status' => $this->stopStatusCode,
                ]);

                return $this->stop($this->stopStatusCode, $this->workerOptions);
            }

            $this->hasJob = false;
        }
    }

    /**
     * @param WorkerOptions $options
     * @param $lastRestart
     * @param $startTime
     * @param $jobsProcessed
     * @param $hasJob
     * @return int|null
     */
    protected function getStopStatus(
        WorkerOptions $options,
        $lastRestart,
        $startTime = 0,
        $jobsProcessed = 0,
        bool $hasJob = false
    ): ?int
    {
        return match (true) {
            $this->shouldQuit => static::EXIT_SUCCESS,
            $this->memoryExceeded($options->memory) => static::EXIT_MEMORY_LIMIT,
            $this->queueShouldRestart($lastRestart) => static::EXIT_SUCCESS,
            $options->stopWhenEmpty && !$hasJob => static::EXIT_SUCCESS,
            $options->maxTime && hrtime(true) / 1e9 - $startTime >= $options->maxTime => static::EXIT_SUCCESS,
            $options->maxJobs && $jobsProcessed >= $options->maxJobs => static::EXIT_SUCCESS,
            default => null
        };
    }

    /**
     * @return RabbitMQQueue
     * @throws Exceptions\MutexTimeout
     */
    protected function startConsuming(): RabbitMQQueue
    {
        $this->processingUuid = $this->generateProcessingUuid();
        $this->processingStartedAt = microtime(true);

        $this->logInfo('startConsuming.init');

        $arguments = [];
        if ($this->maxPriority) {
            $arguments['priority'] = ['I', $this->maxPriority];
        }

        $this->jobsProcessed = 0;

        $connection = $this->initConnection();

        $callback = function (AMQPMessage $message) use ($connection): void {
            $this->hasJob = true;

            $this->processAMQPMessage($message, $connection);

            if ($this->jobsProcessed >= $this->batchSize) {
                $this->stopConsuming();

                $this->processBatch($connection);

                $this->goAheadOrWait();
                $this->startConsuming();
            }

            if ($this->workerOptions->rest > 0) {
                $this->sleep($this->workerOptions->rest);
            }
        };

        $isSuccess = true;

        $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
        try {
            $this->channel->basic_consume(
                $this->currentQueueName,
                $this->getTagName(),
                false,
                false,
                false,
                false,
                $callback,
                null,
                $arguments
            );
        } catch (AMQPProtocolChannelException|AMQPChannelClosedException $exception) {
            $isSuccess = false;

            $this->logError('startConsuming.exception', [
                'message' => $exception->getMessage(),
                'trace' => $exception->getTraceAsString(),
                'error_class' => get_class($exception),
            ]);
        }

        $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);

        $this->updateLastProcessedAt();

        if (false === $isSuccess) {
            $this->stopConsuming();

            $this->goAheadOrWait();
            return $this->startConsuming();
        }

        return $connection;
    }

    /**
     * @return void
     * @throws Exceptions\MutexTimeout
     */
    protected function stopConsuming(): void
    {
        $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
        $this->channel->basic_cancel($this->getTagName(), true);
        $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);
    }
}

