<?php

namespace Salesmessage\LibRabbitMQ;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Factory as QueueManager;
use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use Salesmessage\LibRabbitMQ\Dto\ConsumeVhostsFiltersDto;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Interfaces\RabbitMQBatchable;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJobBatchable;
use Salesmessage\LibRabbitMQ\Queue\VhostsQueueManager as RabbitMQQueueManager;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueueBatchable;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Throwable;

class VhostsConsumer extends Consumer
{
    protected const MAIN_HANDLER_LOCK = 'vhost_handler';

    protected const CONSUME_BATCH_SIZE = 5;

    private ?OutputStyle $output = null;

    private ?ConsumeVhostsFiltersDto $filtersDto = null;

    private string $configConnectionName = '';

    private string $currentConnectionName = '';

    private array $vhosts = [];

    private ?string $currentVhostName = null;

    private array $vhostQueues = [];

    private ?string $currentQueueName = null;

    private ?WorkerOptions $workerOptions = null;

    private bool $hasJob = false;

    private array $batchMessages = [];

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

    /**
     * @param OutputStyle $output
     * @return $this
     */
    public function setOutput(OutputStyle $output): self
    {
        $this->output = $output;
        return $this;
    }

    /**
     * @param ConsumeVhostsFiltersDto $filtersDto
     * @return $this
     */
    public function setFiltersDto(ConsumeVhostsFiltersDto $filtersDto): self
    {
        $this->filtersDto = $filtersDto;
        return $this;
    }

    public function daemon($connectionName, $queue, WorkerOptions $options)
    {
        $this->loadVhosts();
        if (false === $this->switchToNextVhost()) {
            // @todo load vhosts again
            $this->output->warning('No active vhosts... Exit');

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
        $connection = $this->manager->rabbitConnectionByVhost($this->currentVhostName, $this->configConnectionName);
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
            if (false === $this->hasJob) {
                $this->output->info('Consuming sleep. No job...');

                $this->processBatch($connection);

                $this->stopConsuming();
                $this->goAheadOrWait();
                $this->startConsuming();

                $this->sleep($this->workerOptions->sleep);
            }

            // Finally, we will check to see if we have exceeded our memory limits or if
            // the queue should restart based on other indications. If so, we'll stop
            // this worker and let whatever is "monitoring" it restart the process.
            $status = $this->getStopStatus(
                $this->workerOptions,
                $lastRestart,
                $startTime,
                $jobsProcessed,
                $this->hasJob
            );
            if (! is_null($status)) {
                $this->output->info(['Consuming stop.', $status]);

                return $this->stop($status, $this->workerOptions);
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

        $callback = function (AMQPMessage $message) use ($connection, &$jobsProcessed): void {
            $this->hasJob = true;

            if ($this->isSupportBatching($message)) {
                $this->addMessageToBatch($message);
            } else {
                $job = $this->getJobByMessage($message, $connection);
                $this->processSingleJob($job);
            }

            $jobsProcessed++;

            $this->output->info(sprintf(
                'Consume message. Vhost: "%s". Queue: "%s". Num: %s',
                $this->currentVhostName,
                $this->currentQueueName,
                $jobsProcessed
            ));

            if ($jobsProcessed >= self::CONSUME_BATCH_SIZE) {
                $this->processBatch($connection);

                $this->stopConsuming();
                $this->goAheadOrWait();
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
     * @param AMQPMessage $message
     * @return string
     */
    private function getMessageClass(AMQPMessage $message): string
    {
        $body = json_decode($message->getBody(), true);

        return (string) ($body['data']['commandName'] ?? '');
    }

    /**
     * @param RabbitMQJob $job
     * @return void
     */
    private function isSupportBatching(AMQPMessage $message): bool
    {
        $class = $this->getMessageClass($message);

        $reflection = new \ReflectionClass($class);

        return $reflection->implementsInterface(RabbitMQBatchable::class);
    }

    /**
     * @param AMQPMessage $message
     * @return void
     */
    private function addMessageToBatch(AMQPMessage $message): void
    {
        $this->batchMessages[$this->getMessageClass($message)][] = $message;
    }

    /**
     * @param RabbitMQQueue $connection
     * @return void
     * @throws Exceptions\MutexTimeout
     * @throws Throwable
     */
    private function processBatch(RabbitMQQueue $connection): void
    {
        if (empty($this->batchMessages)) {
            return;
        }

        foreach ($this->batchMessages as $batchJobClass => $batchJobMessages) {
            $isBatchSuccess = false;

            $batchSize = count($batchJobMessages);
            if ($batchSize > 1) {
                $batchData = [];
                /** @var AMQPMessage $batchMessage */
                foreach ($batchJobMessages as $batchMessage) {
                    $job = $this->getJobByMessage($batchMessage, $connection);
                    $batchData[] = $job->getPayloadData();
                }

                try {
                    $batchJobClass::collection($batchData);
                    $isBatchSuccess = true;

                    $this->output->comment('Process batch jobs success. Job class: ' . $batchJobClass . 'Size: ' . $batchSize);
                } catch (Throwable $exception) {
                    $isBatchSuccess = false;

                    $this->output->error('Process batch jobs error. Job class: ' . $batchJobClass . ' Error: ' . $exception->getMessage());
                }

                unset($batchData);
            }

            $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
            foreach ($batchJobMessages as $batchMessage) {
                if ($isBatchSuccess) {
                    $this->ackMessage($batchMessage);

                    continue;
                }

                $job = $this->getJobByMessage($batchMessage, $connection);
                $this->processSingleJob($job, $connection);
            }
            $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
        }
        $this->updateLastProcessedAt();

        $this->batchMessages = [];
    }

    /**
     * @param AMQPMessage $message
     * @param RabbitMQQueue $connection
     * @return RabbitMQJob
     * @throws Throwable
     */
    private function getJobByMessage(AMQPMessage $message, RabbitMQQueue $connection): RabbitMQJob
    {
        $jobClass = $connection->getJobClass();

        return new $jobClass(
            $this->container,
            $connection,
            $message,
            $this->currentConnectionName,
            $this->currentQueueName
        );
    }

    /**
     * @param RabbitMQJob $job
     * @return void
     */
    private function processSingleJob(RabbitMQJob $job): void
    {
        if ($this->supportsAsyncSignals()) {
            $this->registerTimeoutHandler($job, $this->workerOptions);
        }

        $this->runJob($job, $this->currentConnectionName, $this->workerOptions);
        $this->updateLastProcessedAt();

        $this->output->info('Process single job...');

        if ($this->supportsAsyncSignals()) {
            $this->resetTimeoutHandler();
        }
    }

    /**
     * @param AMQPMessage $message
     * @return void
     */
    private function ackMessage(AMQPMessage $message): void
    {
        try {
            $message->ack();
        } catch (Throwable $exception) {
            $this->output->error('Ack message error: ' . $exception->getMessage());
        }
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
        $vhosts = $this->internalStorageManager->getVhosts();

        // filter vhosts
        $filterVhosts = $this->filtersDto->getVhosts();
        if (!empty($filterVhosts)) {
            $vhosts = array_filter($vhosts, fn($value) => in_array($value, $filterVhosts, true));
        }
        $this->vhosts = $vhosts;
        $this->vhostQueues = [];

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
            $this->currentVhostName = null;
            $this->currentQueueName = null;
            return false;
        }

        $this->currentVhostName = $nextVhost;
        $this->loadVhostQueues();

        $nextQueue = $this->getNextQueue();
        if (null === $nextQueue) {
            $this->currentQueueName = null;
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
            return !empty($this->vhosts) ? (string) reset($this->vhosts) : null;
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
        $vhostQueues = (null !== $this->currentVhostName)
            ? $this->internalStorageManager->getVhostQueues($this->currentVhostName)
            : [];

        // filter queues
        $filterQueues = $this->filtersDto->getQueues();
        if (!empty($vhostQueues) && !empty($filterQueues)) {
            $vhostQueues = array_filter($vhostQueues, fn($value) => in_array($value, $filterQueues, true));
        }

        $this->vhostQueues = $vhostQueues;

        $this->currentQueueName = null;
    }

    /**
     * @return bool
     */
    private function switchToNextQueue(): bool
    {
        $nextQueue = $this->getNextQueue();
        if (null === $nextQueue) {
            $this->currentQueueName = null;
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
            return !empty($this->vhostQueues) ? (string) reset($this->vhostQueues) : null;
        }

        $currentIndex = array_search($this->currentQueueName, $this->vhostQueues, true);
        if ((false !== $currentIndex) && isset($this->vhostQueues[(int) $currentIndex + 1])) {
            return (string) $this->vhostQueues[(int) $currentIndex + 1];
        }

        return null;
    }

    /**
     * @param int $waitSeconds
     * @return bool
     */
    private function goAheadOrWait(int $waitSeconds = 3): bool
    {
        if (false === $this->goAhead()) {
            $this->loadVhosts();
            if (empty($this->vhosts)) {
                $this->output->warning(sprintf('No active vhosts. Wait %d seconds...', $waitSeconds));
                $this->sleep($waitSeconds);

                return $this->goAheadOrWait($waitSeconds);
            }

            $this->output->info('Starting from the first vhost...');
            return $this->goAheadOrWait($waitSeconds);
        }

        return true;
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

        return false;
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
            'name' => $queueDto->getVhostName(),
        ]);
        $vhostDto->setLastProcessedAt($timestamp);
        $this->internalStorageManager->updateVhostLastProcessedAt($vhostDto);
    }
}

