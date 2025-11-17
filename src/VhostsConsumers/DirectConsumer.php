<?php

namespace Salesmessage\LibRabbitMQ\VhostsConsumers;

use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;

class DirectConsumer extends AbstractVhostsConsumer
{
    /**
     * @param $connectionName
     * @param WorkerOptions $options
     * @return int
     * @throws \Throwable
     */
    protected function vhostDaemon($connectionName, WorkerOptions $options)
    {
        $this->logInfo('daemon.start');

        $this->totalJobsProcessed = 0;

        $connection = $this->startConsuming();
        if ($connection === null) {
            return $this->stopStatusCode;
        }

        while (true) {
            // Before reserving any jobs, we will make sure this queue is not paused and
            // if it is we will just pause this worker for a given amount of time and
            // make sure we do not need to kill this worker process off completely.
            if (! $this->daemonShouldRun($this->workerOptions, $this->configConnectionName, $this->currentQueueName)) {
                $this->logInfo('daemon.consuming_pause_worker');

                $this->pauseWorker($this->workerOptions, $this->lastRestart);

                continue;
            }

            try {
                $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
                $amqpMessage = $this->channel->basic_get($this->currentQueueName);
            } catch (AMQPProtocolChannelException|AMQPChannelClosedException $exception) {
                $amqpMessage = null;

                $this->logError('daemon.channel_exception', [
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                    'error_class' => get_class($exception),
                ]);
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

            if (!isset($amqpMessage)) {
                $this->logInfo('daemon.consuming_sleep_no_job');

                $this->stopConsuming();

                $this->processBatch($connection);

                $goAhead = $this->goAheadOrWait($this->workerOptions->sleep);
                if ($goAhead === false) {
                    return $this->stopStatusCode;
                }
                $connection = $this->startConsuming();
                if ($connection === null) {
                    return $this->stopStatusCode;
                }

                continue;
            }

            $this->processAmqpMessage($amqpMessage, $connection);

            if ($this->jobsProcessed >= $this->batchSize) {
                $this->logInfo('daemon.consuming_batch_full');

                $this->stopConsuming();

                $this->processBatch($connection);

                $goAhead = $this->goAheadOrWait($this->workerOptions->sleep);
                if ($goAhead === false) {
                    return $this->stopStatusCode;
                }
                $connection = $this->startConsuming();
                if ($connection === null) {
                    return $this->stopStatusCode;
                }

                continue;
            }

            // Finally, we will check to see if we have exceeded our memory limits or if
            // the queue should restart based on other indications. If so, we'll stop
            // this worker and let whatever is "monitoring" it restart the process.
            $this->stopStatusCode = $this->stopIfNecessary(
                $this->workerOptions,
                $this->lastRestart,
                $this->startTime,
                $this->totalJobsProcessed,
                true
            );
            if (! is_null($this->stopStatusCode)) {
                $this->logWarning('daemon.consuming_stop', [
                    'status_code' => $this->stopStatusCode,
                ]);

                return $this->stop($this->stopStatusCode, $this->workerOptions);
            }
        }
    }

    protected function startConsuming(): ?RabbitMQQueue
    {
        $this->processingUuid = $this->generateProcessingUuid();
        $this->processingStartedAt = microtime(true);

        $this->logInfo('startConsuming.init');

        $this->jobsProcessed = 0;

        $connection = $this->initConnection();

        $this->updateLastProcessedAt();

        return $connection;
    }

    protected function stopConsuming(): void
    {
        return;
    }
}
