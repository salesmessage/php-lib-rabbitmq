<?php

namespace Salesmessage\LibRabbitMQ\VhostsConsumers;

use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use Salesmessage\LibRabbitMQ\Exceptions\MutexTimeout;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;

class QueueConsumer extends AbstractVhostsConsumer
{
    protected bool $hasJob = false;

    /**
     * @param $connectionName
     * @param WorkerOptions $options
     * @return int|void
     * @throws MutexTimeout
     * @throws \Throwable
     */
    protected function vhostDaemon($connectionName, WorkerOptions $options)
    {
        $this->logInfo('daemon.start');

        $lastRestart = $this->getTimestampOfLastQueueRestart();

        $startTime = hrtime(true) / 1e9;
        $this->totalJobsProcessed = 0;

        $connection = $this->startConsuming();

        while ($this->channel->is_consuming()) {
            // Before reserving any jobs, we will make sure this queue is not paused and
            // if it is we will just pause this worker for a given amount of time and
            // make sure we do not need to kill this worker process off completely.
            if (! $this->daemonShouldRun($this->workerOptions, $this->configConnectionName, $this->currentQueueName)) {
                $this->logInfo('daemon.consuming_pause_worker');

                $this->pauseWorker($this->workerOptions, $lastRestart);

                continue;
            }

            // If the daemon should run (not in maintenance mode, etc.), then we can wait for a job.
            try {
                $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
                $this->channel->wait(null, true, (int) $this->workerOptions->timeout);
            } catch (AMQPRuntimeException $exception) {
                $this->logError('daemon.amqp_runtime_exception', [
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                ]);

                $this->exceptions->report($exception);

                $this->kill(self::EXIT_SUCCESS, $this->workerOptions);
            } catch (\Throwable $exception) {
                $this->logError('daemon.exception', [
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                    'error_class' => get_class($exception),
                ]);

                $this->exceptions->report($exception);

                $this->stopWorkerIfLostConnection($exception);
            } finally {
                $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);
            }

            // If no job is got off the queue, we will need to sleep the worker.
            if (false === $this->hasJob) {
                $this->logInfo('daemon.consuming_sleep_no_job', [
                    'sleep_seconds' => $this->workerOptions->sleep,
                ]);

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
                $this->totalJobsProcessed,
                $this->hasJob
            );
            if (! is_null($this->stopStatusCode)) {
                $this->logWarning('daemon.consuming_stop', [
                    'status_code' => $this->stopStatusCode,
                ]);

                return $this->stop($this->stopStatusCode, $this->workerOptions);
            }

            $this->hasJob = false;
        }
    }

    /**
     * @return RabbitMQQueue
     * @throws MutexTimeout
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
                $this->logInfo('startConsuming.rest', [
                    'rest_seconds' => $this->workerOptions->rest,
                ]);

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
        } finally {
            $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);
        }

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
     * @throws MutexTimeout
     */
    protected function stopConsuming(): void
    {
        $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
        try {
            $this->channel->basic_cancel($this->getTagName(), true);
        } finally {
            $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);
        }
    }
}
