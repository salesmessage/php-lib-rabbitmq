<?php

namespace Salesmessage\LibRabbitMQ\Queue;

use Illuminate\Events\CallQueuedListener;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;
use Salesmessage\LibRabbitMQ\Dto\ConnectionNameDto;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Channel\AMQPChannel;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Lock\LockService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class RabbitMQQueueBatchable extends BaseRabbitMQQueue
{
    private InternalStorageManager $internalStorageManager;

    private GroupsService $groupsService;

    private VhostsService $vhostsService;

    private LockService $lockService;

    private LoggerInterface $logger;

    /**
     * @param QueueConfig $config
     */
    public function __construct(QueueConfig $config)
    {
        $this->internalStorageManager = resolve(InternalStorageManager::class);
        $this->groupsService = resolve(GroupsService::class);
        $this->vhostsService = resolve(VhostsService::class);
        $this->lockService = resolve(LockService::class);
        $this->logger = resolve(LoggerInterface::class);

        parent::__construct($config);
    }

    protected function publishBasic(
        $msg,
        $exchange = '',
        $destination = '',
        $mandatory = false,
        $immediate = false,
        $ticket = null
    ): void {
        $this->withConnectionRetry(
            fn() => parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket)
        );
    }

    protected function publishBatch($jobs, $data = '', $queue = null): void
    {
        // Phase 1: build all AMQPMessages with stable UUIDs - no network I/O here
        $prepared = $this->prepareMessages($jobs, $data, $queue);

        // Phase 2: declare + publish as a single retried unit. The same pre-built
        // messages (and their message_id / deduplication keys) are reused on every
        // reconnect retry; declaration is idempotent and re-run after a reconnect.
        $this->withConnectionRetry(function () use ($prepared): void {
            foreach ($prepared as [$message, $exchange, $destination, $exchangeType, $queueType]) {
                $this->declareDestination($destination, $exchange, $exchangeType, $queueType);
                $this->getChannel()->batch_basic_publish($message, $exchange, $destination);
            }
            $this->getChannel()->publish_batch();
        });
    }

    /**
     * Builds AMQPMessage objects for all jobs without performing any network publish.
     * Separating this from declaration/transport lets reconnect retries reuse the
     * same messages (and therefore the same message_id / deduplication key).
     *
     * @return array<array{0: \PhpAmqpLib\Message\AMQPMessage, 1: string, 2: string, 3: string, 4: string|null, 5: int|string|null}>
     */
    private function prepareMessages($jobs, $data, $queue): array
    {
        $prepared = [];
        foreach ($jobs as $job) {
            if (false === $this->isJobSupported($job)) {
                throw new \RuntimeException('The job is not supported. RabbitMQQueueBatchable.prepareMessages');
            }

            $q = $this->addQueuePostfix($job, $queue);

            // Identical per-job logic to RabbitMQQueue::publishBatch (via bulkRaw),
            // but the message is only built here - declaration and the actual
            // batch_basic_publish happen later so a reconnect retry reuses the same
            // message/UUID.
            $prepared[] = $this->buildMessage(
                $this->createPayload($job, $q, $data),
                $q,
                [
                    'queue_type' => ($job instanceof RabbitMQConsumable) ? $job->getQueueType() : null,
                ]
            );
        }
        return $prepared;
    }

    protected function createChannel(): AMQPChannel
    {
        return $this->withConnectionRetry(fn() => parent::createChannel());
    }

    public function push($job, $data = '', $queue = null)
    {
        $isListenerJob = $job instanceof CallQueuedListener;
        $queue = $isListenerJob
            ? $this->getQueueForListenerJob($job, $queue)
            : $this->getQueueForConsumableJob($job, $queue);

        $options = [
            'queue_type' => ($job instanceof RabbitMQConsumable) ? $job->getQueueType() : null,
        ];

        // Same publish path as RabbitMQQueue::push (postfix + enqueueUsing + pushRaw),
        // but the AMQPMessage is built ONCE up front so the initial attempt and every
        // reconnect retry publish the same message_id - necessary for transport-level
        // deduplication. Declaration + publish are the only retried steps (reusing the
        // pre-built message); parent::publishBasic is called directly to avoid the
        // extra retry layer in the overridden publishBasic.
        $publishQueue = $this->addQueuePostfix($job, $queue);
        $payload = $this->createPayload($job, $this->getQueue($publishQueue), $data);
        [$message, $exchange, $destination, $exchangeType, $queueType, $correlationId] =
            $this->buildMessage($payload, $publishQueue, $options);

        $result = $this->enqueueUsing(
            $job,
            $payload,
            $publishQueue,
            null,
            fn() => $this->withConnectionRetry(function () use ($message, $exchange, $destination, $exchangeType, $queueType, $correlationId) {
                $this->declareDestination($destination, $exchange, $exchangeType, $queueType);
                parent::publishBasic($message, $exchange, $destination, true);

                return $correlationId;
            })
        );

        $this->addQueueToIndex((string) $queue);

        return $result;
    }

    private function getQueueForListenerJob(CallQueuedListener $job, $queue = null)
    {
        if (!$queue) {
            throw new \InvalidArgumentException('Listener must implement viaQueue method');
        }

        return $queue;
    }

    private function getQueueForConsumableJob($job, $queue = null)
    {
        if (!($job instanceof RabbitMQConsumable)) {
            throw new \InvalidArgumentException('Job must implement RabbitMQConsumable interface');
        }

        if ($queue) {
            return $queue;
        }

        $queue = method_exists($job, 'onQueue') ? $job->onQueue() : null;
        if (!$queue) {
            throw new \InvalidArgumentException('Job must implement onQueue method');
        }

        return $queue;
    }

    private function createNotExistsVhost(int $attempts = 0): bool
    {
        $dto = new ConnectionNameDto($this->getConnectionName());
        if (null === $dto->getVhostName()) {
            return false;
        }

        $this->vhostsService->setConnection($dto->getConfigName());

        $hasCreated = false;
        $creationHandler = function () use ($dto, &$hasCreated) {
            $hasCreated = $this->vhostsService->createVhost($dto->getVhostName(), 'Automatically created vhost');
        };

        try {
            $lockKey = 'vhost:' . $dto->getVhostName() . ':creation';
            $handlerWasRun = $this->lockService->lock($lockKey, $creationHandler, skipHandlingOnLock: true);
            // if handler was not run, it means that another process has possibly created the vhost
            // and we need to just re-check if it exists
            if (!$handlerWasRun) {
                $hasCreated = isset($this->vhostsService->getVhost($dto->getVhostName(), 'name')['name']);
            }
        } catch (\Throwable $e) {
            $this->logger->error('RabbitMQQueueBatchable.createNotExistsVhost.exception', [
                'vhost_name' => $dto->getVhostName(),
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
        }

        if (!$hasCreated && $attempts < 2) {
            sleep(1);
            return $this->createNotExistsVhost(++$attempts);
        }

        return $hasCreated;
    }

    /**
     * @param string $queue
     * @return bool
     */
    private function addQueueToIndex(string $queue): bool
    {
        if ('' === $queue) {
            return false;
        }

        $dto = new ConnectionNameDto($this->getConnectionName());
        $vhostName = $dto->getVhostName();
        if ((null === $vhostName)
            || !config('queue.connections.' . $dto->getConfigName() . '.immediate_indexation')
        ) {
            return false;
        }

        $this->internalStorageManager->setConnection($dto->getConfigName());
        $this->groupsService->setConnection($dto->getConfigName());

        $groups = $this->groupsService->getAllGroupsNames();

        $queueApiDto = new QueueApiDto([
            'name' => $queue,
            'vhost' => $vhostName,
        ]);
        $isQueueActivated = $this->internalStorageManager->activateQueue($queueApiDto, $groups);

        $vhostDto = new VhostApiDto([
            'name' => $vhostName,
        ]);
        $isVhostActivated = $this->internalStorageManager->activateVhost($vhostDto, $groups);

        return $isQueueActivated && $isVhostActivated;
    }

    /**
     * @param AMQPConnectionClosedException|AMQPChannelClosedException $exception
     * @return bool
     */
    private function isVhostFailedException(AMQPConnectionClosedException|AMQPChannelClosedException $exception): bool
    {
        $dto = new ConnectionNameDto($this->getConnectionName());
        $vhostName = (string) $dto->getVhostName();

        $notFoundErrorMessage = sprintf('NOT_ALLOWED - vhost %s not found', $vhostName);
        // 530 = connection-level NOT_ALLOWED (vhost missing at connection.open),
        // 403 = channel-level ACCESS_REFUSED for the same missing vhost.
        if (in_array($exception->getCode(), [403, 530], true)
            && str_contains($exception->getMessage(), $notFoundErrorMessage)
        ) {
            return true;
        }

        $deletedErrorMessage = sprintf('CONNECTION_FORCED - vhost \'%s\' is deleted', $vhostName);
        if (str_contains($exception->getMessage(), $deletedErrorMessage)) {
            return true;
        }

        return false;
    }

    /**
     * Run a transport-only operation, retrying it after a connection/channel failure.
     *
     * The callable must contain only network I/O - message building must happen
     * before this call so the same payload (and UUID) is reused on every attempt.
     *
     * @param callable $operation         Transport-only operation to run.
     * @param int      $reconnectAttempts Total reconnect attempts before giving up,
     *                                    passed through to reconnect() (default 1).
     * @param int      $operationAttempts Number of times to retry the operation after a
     *                                    connection failure (0 = run once, no retry).
     */
    private function withConnectionRetry(
        callable $operation,
        int $reconnectAttempts = 2,
        int $operationAttempts = 2
    ): mixed {
        for ($attempt = 0; ; $attempt++) {
            try {
                return $operation();
            } catch (AMQPConnectionClosedException|AMQPChannelClosedException $exception) {
                // No retries left - surface the failure.
                if ($attempt >= $operationAttempts) {
                    throw $exception;
                }

                // Recover before retrying: recreate a missing vhost, then reconnect.
                if ($this->isVhostFailedException($exception) && (false === $this->createNotExistsVhost())) {
                    throw $exception;
                }

                $this->reconnect($reconnectAttempts);
            }
        }
    }
}
