<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Factory as QueueManager;
use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use VladimirYuldashev\LaravelQueueRabbitMQ\Dto\QueueApiDto;
use VladimirYuldashev\LaravelQueueRabbitMQ\Dto\VhostApiDto;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\QueueManager as RabbitMQQueueManager;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueueBatchable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Services\InternalStorageManager;

class VhostsConsumer extends Consumer
{
    protected const MAIN_HANDLER_LOCK = 'vhost_handler';

    private ?OutputStyle $output = null;

    private string $configConnectionName = '';

    private string $currentConnectionName = '';

    private array $vhosts = [];

    private ?string $currentVhostName = null;

    private array $vhostQueues = [];

    private ?string $currentQueueName = null;

    private ?WorkerOptions $workerOptions = null;

    /**
     * @param InternalStorageManager $internalStorageManager
     * @param QueueManager $manager
     * @param Dispatcher $events
     * @param ExceptionHandler $exceptions
     * @param callable $isDownForMaintenance
     * @param callable|null $resetScope
     */
    public function __construct(
        private InternalStorageManager $internalStorageManager,
        QueueManager $manager,
        Dispatcher $events,
        ExceptionHandler $exceptions,
        callable $isDownForMaintenance,
        callable $resetScope = null
    )
    {
        parent::__construct($manager, $events, $exceptions, $isDownForMaintenance, $resetScope);
    }

    public function setOutput(OutputStyle $output)
    {
        $this->output = $output;
    }

    public function daemon($connectionName, $queue, WorkerOptions $options)
    {
        $this->loadVhosts();
        if (false === $this->switchToNextVhost()) {
            // @todo load vhosts again
            $this->output->warning('No active vhosts....');

            return;
        }

        $this->configConnectionName = (string) $connectionName;
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

        $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
        $this->channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            false
        );
        $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);

        $this->startConsuming();

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
                $this->output->error('Consuming AMQP Runtime exception. Error: ' . $exception->getMessage());

                $this->exceptions->report($exception);

                $this->kill(self::EXIT_ERROR, $this->workerOptions);
            } catch (Exception|Throwable $exception) {
                $this->output->error('Consuming exception. Error: ' . $exception->getMessage());

                $this->exceptions->report($exception);

                $this->stopWorkerIfLostConnection($exception);
            }

            // If no job is got off the queue, we will need to sleep the worker.
            if ($this->currentJob === null) {
                $this->output->info('Consuming sleep. No job...');

                $this->stopConsuming();
                $this->goAhead();
                $this->startConsuming();

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
        $this->output->info(sprintf(
            'Start consuming. Vhost: "%s". Queue: "%s"',
            $this->currentVhostName,
            $this->currentQueueName
        ));

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

            $this->output->info(sprintf(
                'Consume message. Vhost: "%s". Queue: "%s". Num: %s',
                $this->currentVhostName,
                $this->currentQueueName,
                $jobsProcessed
            ));

            $this->currentJob = $job;

            if ($this->supportsAsyncSignals()) {
                $this->registerTimeoutHandler($job, $this->workerOptions);
            }

            $jobsProcessed++;

            $this->runJob($job, $this->currentConnectionName, $this->workerOptions);
            $this->updateLastProcessedAt();

            if ($this->supportsAsyncSignals()) {
                $this->resetTimeoutHandler();
            }

            if ($jobsProcessed >= 5) {
                $this->stopConsuming();
                $this->goAhead();
                $this->startConsuming();
            }

            if ($this->workerOptions->rest > 0) {
                $this->sleep($this->workerOptions->rest);
            }
        };

        $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
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
        $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);

        $this->updateLastProcessedAt();
    }

    /**
     * @return void
     * @throws Exceptions\MutexTimeout
     */
    private function stopConsuming()
    {
        $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
        $this->channel->basic_cancel($this->consumerTag, true);
        $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);
    }

    /**
     * @return void
     */
    private function loadVhosts(): void
    {
        $this->vhosts = $this->internalStorageManager->getVhosts();

        $this->currentVhostName = null;
        $this->currentQueueName = null;
    }

    /**
     * @return bool
     */
    private function switchToNextVhost(): bool
    {
        $nextVhost = $this->getNextVhost();
        if (null === $nextVhost) {
            return false;
        }

        $this->currentVhostName = $nextVhost;
        $this->loadVhostQueues();

        $nextQueue = $this->getNextQueue();
        if (null === $nextQueue) {
            return $this->switchToNextVhost();
        }

        $this->currentQueueName = $nextQueue;
        return true;
    }

    /**
     * @return string|null
     */
    private function getNextVhost(): ?string
    {
        if (null === $this->currentVhostName) {
            return (string) reset($this->vhosts);
        }

        $currentIndex = array_search($this->currentVhostName, $this->vhosts, true);
        if ((false !== $currentIndex) && isset($this->vhosts[(int) $currentIndex + 1])) {
            return (string) $this->vhosts[(int) $currentIndex + 1];
        }

        return null;
    }

    /**
     * @return void
     */
    private function loadVhostQueues(): void
    {
        $this->vhostQueues = (null !== $this->currentVhostName)
            ? $this->internalStorageManager->getVhostQueues($this->currentVhostName)
            : [];

        $this->currentQueueName = null;
    }

    /**
     * @return bool
     */
    private function switchToNextQueue(): bool
    {
        $nextQueue = $this->getNextQueue();
        if (null === $nextQueue) {
            return false;
        }

        $this->currentQueueName = $nextQueue;
        return true;
    }

    /**
     * @return string|null
     */
    private function getNextQueue(): ?string
    {
        if (null === $this->currentQueueName) {
            return (string) reset($this->vhostQueues);
        }

        $currentIndex = array_search($this->currentQueueName, $this->vhostQueues, true);
        if ((false !== $currentIndex) && isset($this->vhostQueues[(int) $currentIndex + 1])) {
            return (string) $this->vhostQueues[(int) $currentIndex + 1];
        }

        return null;
    }

    /**
     * @return bool
     */
    private function goAhead(): bool
    {
        if ($this->switchToNextQueue()) {
            return true;
        }

        if ($this->switchToNextVhost()) {
            return true;
        }

        $this->loadVhosts();
        return $this->switchToNextVhost();
    }

    /**
     * @return void
     */
    private function updateLastProcessedAt()
    {
        if ((null === $this->currentVhostName) || (null === $this->currentQueueName)) {
            return;
        }

        $timestamp = time();

        $queueDto = new QueueApiDto([
            'name' => $this->currentQueueName,
            'vhost' => $this->currentVhostName,
        ]);
        $queueDto->setLastProcessedAt($timestamp);
        $this->internalStorageManager->updateQueueLastProcessedAt($queueDto);

        $vhostDto = new VhostApiDto([
            'name' => $queueDto->getVhostName()
        ]);
        $vhostDto->setLastProcessedAt($timestamp);
        $this->internalStorageManager->updateVhostLastProcessedAt($vhostDto);
    }
}

