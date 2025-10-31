<?php

namespace Salesmessage\LibRabbitMQ\VhostsConsumers;

use Illuminate\Console\OutputStyle;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Support\Str;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Consumer;
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;
use Salesmessage\LibRabbitMQ\Dto\ConnectionNameDto;
use Salesmessage\LibRabbitMQ\Dto\ConsumeVhostsFiltersDto;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Exceptions\MutexTimeout;
use Salesmessage\LibRabbitMQ\Interfaces\RabbitMQBatchable;
use Salesmessage\LibRabbitMQ\Mutex;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\Deduplication\AppDeduplicationService;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService as TransportDeduplicationService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;

abstract class AbstractVhostsConsumer extends Consumer
{
    protected const MAIN_HANDLER_LOCK = 'vhost_handler';

    protected const HEALTHCHECK_HANDLER_LOCK = 'healthcheck_vhost_handler';

    protected ?OutputStyle $output = null;

    protected ?ConsumeVhostsFiltersDto $filtersDto = null;

    protected int $batchSize = 100;

    protected string $configConnectionName = '';

    protected string $currentConnectionName = '';

    protected array $vhosts = [];

    protected ?string $currentVhostName = null;

    protected array $vhostQueues = [];

    protected ?string $currentQueueName = null;

    protected ?WorkerOptions $workerOptions = null;

    /** @var array<class-string<RabbitMQBatchable>, array<AMQPMessage>> */
    protected array $batchMessages = [];

    protected ?string $processingUuid = null;

    protected int|float $processingStartedAt = 0;

    protected int $totalJobsProcessed = 0;

    protected int $jobsProcessed = 0;

    protected bool $hadJobs = false;

    protected ?int $stopStatusCode = null;

    protected array $config = [];

    protected bool $asyncMode = false;

    protected ?Mutex $connectionMutex = null;

    /**
     * @param InternalStorageManager $internalStorageManager
     * @param LoggerInterface $logger
     * @param QueueManager $manager
     * @param Dispatcher $events
     * @param ExceptionHandler $exceptions
     * @param callable $isDownForMaintenance
     * @param TransportDeduplicationService $transportDeduplicationService
     * @param callable|null $resetScope
     */
    public function __construct(
        protected InternalStorageManager $internalStorageManager,
        protected LoggerInterface $logger,
        QueueManager $manager,
        Dispatcher $events,
        ExceptionHandler $exceptions,
        callable $isDownForMaintenance,
        protected TransportDeduplicationService $transportDeduplicationService,
        callable $resetScope = null,
    ) {
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

    /**
     * @param int $batchSize
     * @return $this
     */
    public function setBatchSize(int $batchSize): self
    {
        $this->batchSize = $batchSize;
        return $this;
    }

    /**
     * @param array $config
     * @return $this
     */
    public function setConfig(array $config): self
    {
        $this->config = $config;
        return $this;
    }

    /**
     * @param bool $asyncMode
     * @return $this
     */
    public function setAsyncMode(bool $asyncMode): self
    {
        $this->asyncMode = $asyncMode;
        return $this;
    }

    public function daemon($connectionName, $queue, WorkerOptions $options)
    {
        $this->goAheadOrWait();

        $this->configConnectionName = (string) $connectionName;
        $this->workerOptions = $options;

        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        if ($this->asyncMode) {
            $this->logInfo('daemon.AsyncMode.On');

            $coroutineContextHandler = function () use ($connectionName, $options) {
                $this->logInfo('daemon.AsyncMode.Coroutines.Running');

                // we can't move it outside since Mutex should be created within coroutine context
                $this->connectionMutex = new Mutex(true);
                $this->startHeartbeatCheck();
                \go(function () use ($connectionName, $options) {
                    $this->vhostDaemon($connectionName, $options);
                });
            };

            if (extension_loaded('swoole')) {
                $this->logInfo('daemon.AsyncMode.Swoole');

                \Co\run($coroutineContextHandler);
            } elseif (extension_loaded('openswoole')) {
                $this->logInfo('daemon.AsyncMode.OpenSwoole');

                \OpenSwoole\Runtime::enableCoroutine(true, \OpenSwoole\Runtime::HOOK_ALL);
                \co::run($coroutineContextHandler);
            } else {
                $this->logError('daemon.AsyncMode.IsNotSupported');

                throw new \Exception('Async mode is not supported. Check if Swoole extension is installed');
            }

            return;
        }

        $this->logInfo('daemon.AsyncMode.Off');

        $this->connectionMutex = new Mutex(false);
        $this->startHeartbeatCheck();
        $this->vhostDaemon($connectionName, $options);
    }

    abstract protected function vhostDaemon($connectionName, WorkerOptions $options);

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
    ): ?int {
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
     * @throws MutexTimeout
     */
    abstract protected function startConsuming(): RabbitMQQueue;

    /**
     * @param AMQPMessage $message
     * @param RabbitMQQueue $connection
     * @return void
     */
    protected function processAmqpMessage(AMQPMessage $message, RabbitMQQueue $connection): void
    {
        $this->hadJobs = true;
        $isSupportBatching = $this->isSupportBatching($message);
        if ($isSupportBatching) {
            $this->addMessageToBatch($message);
        } else {
            $job = $this->getJobByMessage($message, $connection);
            $this->processSingleJob($job, $message);
        }

        $this->jobsProcessed++;
        $this->totalJobsProcessed++;

        $this->logInfo('processAMQPMessage.message_consumed', [
            'processed_jobs_count' => $this->jobsProcessed,
            'is_support_batching' => $isSupportBatching ? 'Y' :'N',
        ]);
    }

    /**
     * @return string
     */
    protected function generateProcessingUuid(): string
    {
        return sprintf('%s:%d:%s', $this->filtersDto->getGroup(), time(), Str::random(16));
    }

    /**
     * @param AMQPMessage $message
     * @return non-empty-string
     */
    protected function getMessageClass(AMQPMessage $message): string
    {
        $body = json_decode($message->getBody(), true);

        $messageClass = (string) ($body['data']['commandName'] ?? '');
        if (empty($messageClass)) {
            throw new \RuntimeException('Message class is not defined');
        }
        return $messageClass;
    }

    /**
     * @param AMQPMessage $message
     * @return bool
     * @throws \ReflectionException
     */
    protected function isSupportBatching(AMQPMessage $message): bool
    {
        $class = $this->getMessageClass($message);

        $reflection = new \ReflectionClass($class);

        return $reflection->implementsInterface(RabbitMQBatchable::class);
    }

    /**
     * @param AMQPMessage $message
     * @return void
     */
    protected function addMessageToBatch(AMQPMessage $message): void
    {
        $this->batchMessages[$this->getMessageClass($message)][] = $message;
    }

    /**
     * @param RabbitMQQueue $connection
     * @return void
     * @throws MutexTimeout
     * @throws \Throwable
     */
    protected function processBatch(RabbitMQQueue $connection): void
    {
        if (empty($this->batchMessages)) {
            return;
        }

        foreach ($this->batchMessages as $batchJobClass => $batchJobMessages) {
            $isBatchSuccess = false;
            $batchSize = count($batchJobMessages);

            if ($batchSize > 1) {
                $batchTimeStarted = microtime(true);

                $uniqueMessagesForProcessing = [];
                $batchData = [];
                foreach ($batchJobMessages as $batchMessage) {
                    $this->transportDeduplicationService->decorateWithDeduplication(
                        function () use ($batchMessage, $connection, &$uniqueMessagesForProcessing, &$batchData) {
                            $job = $this->getJobByMessage($batchMessage, $connection);
                            $uniqueMessagesForProcessing[] = $batchMessage;
                            $batchData[] = $job->getPayloadData();
                        },
                        $batchMessage,
                        $this->currentQueueName
                    );
                }

                try {
                    if (AppDeduplicationService::isEnabled()) {
                        /** @var RabbitMQBatchable $batchJobClass */
                        $batchData = $batchJobClass::getNotDuplicatedBatchedJobs($batchData);
                    }

                    if (!empty($batchData)) {
                        $this->logInfo('processBatch.start', [
                            'batch_job_class' => $batchJobClass,
                            'batch_size' => $batchSize,
                        ]);

                        $batchJobClass::collection($batchData);

                        $this->logInfo('processBatch.finish', [
                            'batch_job_class' => $batchJobClass,
                            'batch_size' => $batchSize,
                            'executive_batch_time_seconds' => microtime(true) - $batchTimeStarted,
                        ]);
                    }

                    $isBatchSuccess = true;
                } catch (\Throwable $exception) {
                    foreach ($uniqueMessagesForProcessing as $batchMessage) {
                        $this->transportDeduplicationService->release($batchMessage, $this->currentQueueName);
                    }

                    $isBatchSuccess = false;

                    $this->logError('processBatch.exception', [
                        'batch_job_class' => $batchJobClass,
                        'message' => $exception->getMessage(),
                        'trace' => $exception->getTraceAsString(),
                        'error_class' => get_class($exception),
                    ]);
                }

                unset($batchData);
            } else {
                $uniqueMessagesForProcessing = $batchJobMessages;
            }

            $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
            try {
                if ($isBatchSuccess) {
                    foreach ($uniqueMessagesForProcessing as $batchMessage) {
                        $this->transportDeduplicationService?->markAsProcessed($batchMessage, $this->currentQueueName);
                    }

                    $lastBatchMessage = end($uniqueMessagesForProcessing);
                    $this->ackMessage($lastBatchMessage, true);
                } else {
                    foreach ($uniqueMessagesForProcessing as $batchMessage) {
                        $job = $this->getJobByMessage($batchMessage, $connection);
                        $this->processSingleJob($job, $batchMessage);
                    }
                }
            } finally {
                $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
            }
        }
        $this->updateLastProcessedAt();

        $this->batchMessages = [];
    }

    /**
     * @param AMQPMessage $message
     * @param RabbitMQQueue $connection
     * @return RabbitMQJob
     * @throws \Throwable
     */
    protected function getJobByMessage(AMQPMessage $message, RabbitMQQueue $connection): RabbitMQJob
    {
        $jobClass = $connection->getJobClass();

        $job = new $jobClass(
            $this->container,
            $connection,
            $message,
            $this->currentConnectionName,
            $this->currentQueueName
        );

        if (!is_subclass_of($job->getPayloadClass(), RabbitMQConsumable::class)) {
            throw new \RuntimeException(sprintf('Job class %s must implement %s', $job->getPayloadClass(), RabbitMQConsumable::class));
        }

        return $job;
    }

    protected function processSingleJob(RabbitMQJob $job, AMQPMessage $message): void
    {
        $timeStarted = microtime(true);
        $this->logInfo('processSingleJob.start');

        if ($this->supportsAsyncSignals()) {
            $this->registerTimeoutHandler($job, $this->workerOptions);
        }

        $this->transportDeduplicationService->decorateWithDeduplication(
            function () use ($job, $message) {
                if (AppDeduplicationService::isEnabled() && $job->getPayloadData()->isDuplicated()) {
                    $this->logWarning('processSingleJob.job_is_duplicated');
                    $this->ackMessage($message);

                } else {
                    $this->runJob($job, $this->currentConnectionName, $this->workerOptions);
                }

                $this->transportDeduplicationService->markAsProcessed($message, $this->currentQueueName);
            },
            $message,
            $this->currentQueueName,
        );

        $this->updateLastProcessedAt();

        if ($this->supportsAsyncSignals()) {
            $this->resetTimeoutHandler();
        }

        $this->logInfo('processSingleJob.finish', [
            'executive_job_time_seconds' => microtime(true) - $timeStarted,
        ]);
    }

    /**
     * @param AMQPMessage $message
     * @param bool $multiple
     * @return void
     */
    protected function ackMessage(AMQPMessage $message, bool $multiple = false): void
    {
        $this->logInfo('ackMessage.start', [
            'multiple' => $multiple,
        ]);

        try {
            $message->ack($multiple);
        } catch (\Throwable $exception) {
            $this->logError('ackMessage.exception', [
                'message' => $exception->getMessage(),
                'trace' => $exception->getTraceAsString(),
                'error_class' => get_class($exception),
            ]);
        }
    }

    /**
     * @return void
     * @throws MutexTimeout
     */
    abstract protected function stopConsuming(): void;

    /**
     * @return void
     */
    protected function loadVhosts(): void
    {
        $this->logInfo('loadVhosts.start');

        $group = $this->filtersDto->getGroup();
        $lastProcessedAtKey = $this->internalStorageManager->getLastProcessedAtKeyName($group);

        $vhosts = $this->internalStorageManager->getVhosts($lastProcessedAtKey, false);

        // filter vhosts
        $filterVhosts = $this->filtersDto->getVhosts();
        if (!empty($filterVhosts)) {
            $vhosts = array_filter($vhosts, fn($value) => in_array($value, $filterVhosts, true));
        }

        // filter vhosts mask
        $filterVhostsMask = $this->filtersDto->getVhostsMask();
        if ('' !== $filterVhostsMask) {
            $vhosts = array_filter($vhosts, fn($value) => str_contains($value, $filterVhostsMask));
        }

        $this->vhosts = $vhosts;
        $this->vhostQueues = [];

        $this->currentVhostName = null;
        $this->currentQueueName = null;
    }

    /**
     * @return bool
     */
    protected function switchToNextVhost(): bool
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

        $this->logInfo('switchToNextVhost.success');

        return true;
    }

    /**
     * @return string|null
     */
    protected function getNextVhost(): ?string
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
    protected function loadVhostQueues(): void
    {
        $this->logInfo('loadVhostQueues.start');

        $group = $this->filtersDto->getGroup();
        $lastProcessedAtKey = $this->internalStorageManager->getLastProcessedAtKeyName($group);

        $vhostQueues = (null !== $this->currentVhostName)
            ? $this->internalStorageManager->getVhostQueues($this->currentVhostName, $lastProcessedAtKey, false)
            : [];

        // filter queues
        $filterQueues = $this->filtersDto->getQueues();
        if (!empty($vhostQueues) && !empty($filterQueues)) {
            $vhostQueues = array_filter($vhostQueues, fn($value) => in_array($value, $filterQueues, true));
        }

        // filter queues mask
        $filterQueuesMask = $this->filtersDto->getQueuesMask();
        if ('' !== $filterQueuesMask) {
            $vhostQueues = array_filter($vhostQueues, fn($value) => str_contains($value, $filterQueuesMask));
        }

        $this->vhostQueues = $vhostQueues;

        $this->currentQueueName = null;
    }

    /**
     * @return bool
     */
    protected function switchToNextQueue(): bool
    {
        $nextQueue = $this->getNextQueue();
        if (null === $nextQueue) {
            $this->currentQueueName = null;
            return false;
        }

        $this->currentQueueName = $nextQueue;

        $this->logInfo('switchToNextQueue.success');

        return true;
    }

    /**
     * @return string|null
     */
    protected function getNextQueue(): ?string
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
    protected function goAheadOrWait(int $waitSeconds = 1): bool
    {
        if (false === $this->goAhead()) {
            if (!$this->hadJobs) {
                $this->logWarning('goAheadOrWait.no_jobs_during_iteration', [
                    'wait_seconds' => $waitSeconds,
                ]);

                $this->sleep($waitSeconds);
            }

            $this->loadVhosts();
            $this->hadJobs = false;
            if (empty($this->vhosts)) {
                $this->logWarning('goAheadOrWait.no_active_vhosts', [
                    'wait_seconds' => $waitSeconds,
                ]);

                $this->sleep($waitSeconds);

                return $this->goAheadOrWait($waitSeconds);
            }

            $this->logInfo('goAheadOrWait.starting_from_the_first_vhost');
            return $this->goAheadOrWait($waitSeconds);
        }

        return true;
    }

    /**
     * @return bool
     */
    protected function goAhead(): bool
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
    protected function updateLastProcessedAt(): void
    {
        if ((null === $this->currentVhostName) || (null === $this->currentQueueName)) {
            return;
        }

        $this->logInfo('updateLastProcessedAt.start');

        $group = $this->filtersDto->getGroup();
        $timestamp = time();

        $queueDto = new QueueApiDto([
            'name' => $this->currentQueueName,
            'vhost' => $this->currentVhostName,
        ]);
        $queueDto
            ->setGroupName($group)
            ->setLastProcessedAt($timestamp);
        $this->internalStorageManager->updateQueueLastProcessedAt($queueDto);

        $vhostDto = new VhostApiDto([
            'name' => $queueDto->getVhostName(),
        ]);
        $vhostDto
            ->setGroupName($group)
            ->setLastProcessedAt($timestamp);
        $this->internalStorageManager->updateVhostLastProcessedAt($vhostDto);
    }

    /**
     * @return RabbitMQQueue
     */
    protected function initConnection(): RabbitMQQueue
    {
        if ($this->channel) {
            try {
                $this->channel->close();
            } catch (\Exception $e) {
                // Ignore close errors
            }
            $this->channel = null;
        }

        $connection = $this->manager->connection(
            ConnectionNameDto::getVhostConnectionName($this->currentVhostName, $this->configConnectionName)
        );

        try {
            /** @var AMQPChannel $channel */
            $channel = $connection->getChannel(true);

            $this->currentConnectionName = $connection->getConnectionName();

            $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
            $channel->basic_qos(
                $this->prefetchSize,
                $this->prefetchCount,
                false
            );

            $this->channel = $channel;
            $this->connection = $connection;
        } catch (AMQPConnectionClosedException | AMQPChannelClosedException $exception) {
            $this->logError('initConnection.exception', [
                'message' => $exception->getMessage(),
                'trace' => $exception->getTraceAsString(),
            ]);

            $vhostDto = new VhostApiDto([
                'name' => $this->currentVhostName,
            ]);

            $this->internalStorageManager->removeVhost($vhostDto);
            $this->loadVhosts();
            $this->goAheadOrWait();

            return $this->initConnection();
        } finally {
            $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);
        }

        return $connection;
    }

    /**
     * @return void
     */
    protected function startHeartbeatCheck(): void
    {
        if (false === $this->asyncMode) {
            return;
        }

        $heartbeatInterval = (int) ($this->config['options']['heartbeat'] ?? 0);
        if (!$heartbeatInterval) {
            $this->logWarning('startHeartbeatCheck.heartbeat_interval_is_not_set');
            return;
        }

        $this->logInfo('startHeartbeatCheck.start', [
            'heartbeat_interval' => $heartbeatInterval,
        ]);

        $heartbeatHandler = function () {
            if ($this->shouldQuit || (null !== $this->stopStatusCode)) {
                $this->logWarning('startHeartbeatCheck.quit', [
                    'should_quit' => $this->shouldQuit,
                    'stop_status_code' => $this->stopStatusCode,
                ]);

                return;
            }

            try {
                /** @var AMQPStreamConnection|null $connection */
                $connection = $this->connection?->getConnection();
                if ((null === $connection)
                    || (false === $connection->isConnected())
                    || $connection->isWriting()
                    || $connection->isBlocked()
                ) {
                    $this->logWarning('startHeartbeatCheck.incorrect_connection', [
                        'has_connection' => (null !== $connection) ? 'Y' : 'N',
                        'is_connected' => $connection?->isConnected() ? 'Y' : 'N',
                        'is_writing' => $connection->isWriting() ? 'Y' : 'N',
                        'is_blocked' => $connection->isBlocked() ? 'Y' : 'N',
                    ]);

                    return;
                }

                $this->connectionMutex->lock(static::HEALTHCHECK_HANDLER_LOCK, 3);
                $connection->checkHeartBeat();
            } catch (MutexTimeout) {
                $this->logWarning('startHeartbeatCheck.mutex_timeout');
            } catch (\Throwable $exception) {
                $this->logError('startHeartbeatCheck.exception', [
                    'error' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                ]);

                $this->shouldQuit = true;
            } finally {
                $this->connectionMutex->unlock(static::HEALTHCHECK_HANDLER_LOCK);
            }
        };

        \go(function () use ($heartbeatHandler, $heartbeatInterval) {
            $this->logInfo('startHeartbeatCheck.started');

            while (true) {
                sleep($heartbeatInterval);
                $heartbeatHandler();
                if ($this->shouldQuit || !is_null($this->stopStatusCode)) {
                    $this->logWarning('startHeartbeatCheck.go_quit', [
                        'should_quit' => $this->shouldQuit,
                        'stop_status_code' => $this->stopStatusCode,
                    ]);

                    return;
                }
            }
        });
    }

    /**
     * @return string
     */
    protected function getTagName(): string
    {
        return $this->consumerTag . '_' .  $this->currentVhostName;
    }

    /**
     * @param string $message
     * @param array $data
     * @return void
     */
    protected function logInfo(string $message, array $data = []): void
    {
        $this->log($message, $data, 'info');
    }

    /**
     * @param string $message
     * @param array $data
     * @return void
     */
    protected function logWarning(string $message, array $data = []): void
    {
        $this->log($message, $data, 'warning');
    }

    /**
     * @param string $message
     * @param array $data
     * @return void
     */
    protected function logError(string $message, array $data = []): void
    {
        $this->log($message, $data, 'error');
    }

    /**
     * @param string $message
     * @param array $data
     * @param string $logType
     * @return void
     */
    protected function log(string $message, array $data = [], string $logType = 'info'): void
    {
        if (null !== $this->currentVhostName) {
            $data['vhost_name'] = $this->currentVhostName;
        }
        if (null !== $this->currentQueueName) {
            $data['queue_name'] = $this->currentQueueName;
        }

        $outputMessage = $message;
        foreach ($data as $key => $value) {
            if (in_array($key, ['trace', 'error_class'])) {
                continue;
            }
            $outputMessage .= '. ' . ucfirst(str_replace('_', ' ', $key)) . ': ' . $value;
        }

        match ($logType) {
            'error' => $this->output->error($outputMessage),
            'warning' => $this->output->warning($outputMessage),
            default => $this->output->info($outputMessage)
        };

        $processingData = [
            'uuid' => $this->processingUuid,
            'started_at' => $this->processingStartedAt,
            'total_processed_jobs_count' => $this->totalJobsProcessed,
        ];
        if ($this->processingStartedAt) {
            $processingData['executive_time_seconds'] = microtime(true) - $this->processingStartedAt;
        }
        $data['processing'] = $processingData;

        $logMessage = 'Salesmessage.LibRabbitMQ.VhostsConsumers.';
        $logMessage .= class_basename(static::class) . '.';
        $logMessage .= $message;

        match ($logType) {
            'error' => $this->logger->error($logMessage, $data),
            'warning' => $this->logger->warning($logMessage, $data),
            default => $this->logger->info($logMessage, $data)
        };
    }
}
