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

class DirectConsumer extends AbstractVhostsConsumer
{
    protected function vhostDaemon($connectionName, WorkerOptions $options)
    {
        $lastRestart = $this->getTimestampOfLastQueueRestart();

        [$startTime, $jobsProcessed] = [hrtime(true) / 1e9, 0];

        $connection = $this->startConsuming();

        while (true) {
            // Before reserving any jobs, we will make sure this queue is not paused and
            // if it is we will just pause this worker for a given amount of time and
            // make sure we do not need to kill this worker process off completely.
            if (! $this->daemonShouldRun($this->workerOptions, $this->configConnectionName, $this->currentQueueName)) {
                $this->output->info('Consuming pause worker...');

                $this->pauseWorker($this->workerOptions, $lastRestart);

                continue;
            }

            try {
                $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
                $amqpMessage = $this->channel->basic_get($this->currentQueueName);
                if (null !== $amqpMessage) {
                    $this->channel->basic_reject($amqpMessage->getDeliveryTag(), false);
                }
                $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);
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
            } catch (Exception|Throwable $exception) {
                $this->logError('daemon.exception', [
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                    'error_class' => get_class($exception),
                ]);

                $this->exceptions->report($exception);

                $this->stopWorkerIfLostConnection($exception);
            }

            if (null === $amqpMessage) {
                $this->output->info('Consuming sleep. No job...');

                $this->stopConsuming();

                $this->processBatch($connection);

                $this->goAheadOrWait();
                $connection = $this->startConsuming();

                continue;
            }

            $this->processAmqpMessage($amqpMessage, $connection);

            if ($this->jobsProcessed >= $this->batchSize) {
                $this->output->info('Consuming batch full...');

                $this->stopConsuming();

                $this->processBatch($connection);

                $this->goAheadOrWait();
                $connection = $this->startConsuming();

                continue;
            }
        }
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

