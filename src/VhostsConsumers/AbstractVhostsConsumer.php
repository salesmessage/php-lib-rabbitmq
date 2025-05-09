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
use Salesmessage\LibRabbitMQ\Consumer;
use Salesmessage\LibRabbitMQ\Dto\ConnectionNameDto;
use Salesmessage\LibRabbitMQ\Dto\ConsumeVhostsFiltersDto;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Interfaces\RabbitMQBatchable;
use Salesmessage\LibRabbitMQ\Mutex;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;

abstract class AbstractVhostsConsumer extends Consumer
{
    protected const MAIN_HANDLER_LOCK = 'vhost_handler';

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

    protected array $batchMessages = [];

    protected ?string $processingUuid = null;

    protected int|float $processingStartedAt = 0;

    protected int $jobsProcessed = 0;

    /**
     * @param InternalStorageManager $internalStorageManager
     * @param LoggerInterface $logger
     * @param QueueManager $manager
     * @param Dispatcher $events
     * @param ExceptionHandler $exceptions
     * @param callable $isDownForMaintenance
     * @param callable|null $resetScope
     */
    public function __construct(
        protected InternalStorageManager $internalStorageManager,
        protected LoggerInterface $logger,
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

    /**
     * @param int $batchSize
     * @return $this
     */
    public function setBatchSize(int $batchSize): self
    {
        $this->batchSize = $batchSize;
        return $this;
    }

    public function daemon($connectionName, $queue, WorkerOptions $options)
    {
        $this->goAheadOrWait();

        $this->connectionMutex = new Mutex(false);

        $this->configConnectionName = (string) $connectionName;
        $this->workerOptions = $options;

        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

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
    abstract protected function startConsuming(): RabbitMQQueue;

    /**
     * @param AMQPMessage $message
     * @param RabbitMQQueue $connection
     * @return void
     */
    protected function processAmqpMessage(AMQPMessage $message, RabbitMQQueue $connection): void
    {
        $isSupportBatching = $this->isSupportBatching($message);
        if ($isSupportBatching) {
            $this->addMessageToBatch($message);
        } else {
            $job = $this->getJobByMessage($message, $connection);
            $this->processSingleJob($job);
        }

        $this->jobsProcessed++;

        $this->logInfo('processAMQPMessage.message_consumed', [
            'processed_jobs_count' => $this->jobsProcessed,
            'is_support_batching' => $isSupportBatching,
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
     * @return string
     */
    protected function getMessageClass(AMQPMessage $message): string
    {
        $body = json_decode($message->getBody(), true);

        return (string) ($body['data']['commandName'] ?? '');
    }

    /**
     * @param RabbitMQJob $job
     * @return void
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
     * @throws Exceptions\MutexTimeout
     * @throws Throwable
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

                $batchData = [];
                /** @var AMQPMessage $batchMessage */
                foreach ($batchJobMessages as $batchMessage) {
                    $job = $this->getJobByMessage($batchMessage, $connection);
                    $batchData[] = $job->getPayloadData();
                }

                $this->logInfo('processBatch.start', [
                    'batch_job_class' => $batchJobClass,
                    'batch_size' => $batchSize,
                ]);

                try {
                    $batchJobClass::collection($batchData);
                    $isBatchSuccess = true;

                    $this->logInfo('processBatch.finish', [
                        'batch_job_class' => $batchJobClass,
                        'batch_size' => $batchSize,
                        'executive_batch_time_seconds' => microtime(true) - $batchTimeStarted,
                    ]);
                } catch (Throwable $exception) {
                    $isBatchSuccess = false;

                    $this->logError('processBatch.exception', [
                        'batch_job_class' => $batchJobClass,
                        'message' => $exception->getMessage(),
                        'trace' => $exception->getTraceAsString(),
                        'error_class' => get_class($exception),
                    ]);
                }

                unset($batchData);
            }

            $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
            if ($isBatchSuccess) {
                $lastBatchMessage = end($batchJobMessages);
                $this->ackMessage($lastBatchMessage, true);
            } else {
                foreach ($batchJobMessages as $batchMessage) {
                    $job = $this->getJobByMessage($batchMessage, $connection);
                    $this->processSingleJob($job);
                }
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
    protected function getJobByMessage(AMQPMessage $message, RabbitMQQueue $connection): RabbitMQJob
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
    protected function processSingleJob(RabbitMQJob $job): void
    {
        $timeStarted = microtime(true);
        $this->logInfo('processSingleJob.start');

        if ($this->supportsAsyncSignals()) {
            $this->registerTimeoutHandler($job, $this->workerOptions);
        }

        $this->runJob($job, $this->currentConnectionName, $this->workerOptions);
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
        try {
            $message->ack($multiple);
        } catch (Throwable $exception) {
            $this->logError('ackMessage.exception', [
                'message' => $exception->getMessage(),
                'trace' => $exception->getTraceAsString(),
                'error_class' => get_class($exception),
            ]);
        }
    }

    /**
     * @return void
     * @throws Exceptions\MutexTimeout
     */
    abstract protected function stopConsuming(): void;

    /**
     * @return void
     */
    protected function loadVhosts(): void
    {
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
    protected function updateLastProcessedAt()
    {
        if ((null === $this->currentVhostName) || (null === $this->currentQueueName)) {
            return;
        }

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
        // Close any existing connection/channel
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
            $channel = $connection->getChannel();
        } catch (AMQPConnectionClosedException $exception) {
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
        }

        $this->currentConnectionName = $connection->getConnectionName();

        $this->connectionMutex->lock(self::MAIN_HANDLER_LOCK);
        $channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            false
        );
        $this->connectionMutex->unlock(self::MAIN_HANDLER_LOCK);

        $this->channel = $channel;

        return $connection;
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
        $this->log($message, $data, false);
    }

    /**
     * @param string $message
     * @param array $data
     * @return void
     */
    protected function logError(string $message, array $data = []): void
    {
        $this->log($message, $data, true);
    }

    /**
     * @param string $message
     * @param array $data
     * @param bool $isError
     * @return void
     */
    protected function log(string $message, array $data = [], bool $isError = false): void
    {
        $data['vhost_name'] = $this->currentVhostName;
        $data['queue_name'] = $this->currentQueueName;

        $outputMessage = $message;
        foreach ($data as $key => $value) {
            if (in_array($key, ['trace', 'error_class'])) {
                continue;
            }
            $outputMessage .= '. ' . ucfirst(str_replace('_', ' ', $key)) . ': ' . $value;
        }
        if ($isError) {
            $this->output->error($outputMessage);
        } else {
            $this->output->info($outputMessage);
        }

        $processingData = [
            'uuid' => $this->processingUuid,
            'started_at' => $this->processingStartedAt,
        ];
        if ($this->processingStartedAt) {
            $processingData['executive_time_seconds'] = microtime(true) - $this->processingStartedAt;
        }
        $data['processing'] = $processingData;

        $logMessage = 'Salesmessage.LibRabbitMQ.VhostsConsumers.';
        $logMessage .= class_basename(static::class) . '.';
        $logMessage .= $message;
        if ($isError) {
            $this->logger->error($logMessage, $data);
        } else {
            $this->logger->info($logMessage, $data);
        }
    }
}