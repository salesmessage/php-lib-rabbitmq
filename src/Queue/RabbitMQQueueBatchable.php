<?php

namespace Salesmessage\LibRabbitMQ\Queue;

use Illuminate\Events\CallQueuedListener;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;
use Salesmessage\LibRabbitMQ\Dto\ConnectionNameDto;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Exceptions\RabbitVhostsGroupsException;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;
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
     * Payload and idle time of the publish in progress, so a reconnect logged for it names its job. The
     * payload is decoded only when a reconnect is logged, not on every publish.
     */
    private ?string $publishingPayload = null;

    private ?float $publishingIdleSeconds = null;

    /**
     * Retried units running (a depth, so units that interleave can't leave it set): an inner retry (opening a
     * channel, a publish) inside a unit only runs its operation, and the unit's retry recovers and runs the
     * whole unit again, declarations included.
     */
    private int $runningUnits = 0;

    /** A write timed out and may have left part of a frame in the stream: reconnect before the next unit. */
    private bool $replaceConnection = false;

    /** Recoveries of a publish unit: one per step it has (open a channel, declare, publish) and one more. */
    private const PUBLISH_RECOVERIES = 4;

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
        $ticket = null,
        bool $confirm = false
    ): void {
        $this->withConnectionRetry(
            fn() => parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket, $confirm),
            payload: $msg->getBody(),
            inner: true
        );
    }

    /**
     * pushRaw() and laterRaw() declare and publish as one retried unit, as push() and publishBatch() do:
     * after a reconnect the queues are declared again, which an empty recreated vhost needs.
     */
    protected function publishTransport(callable $transport, string $payload): mixed
    {
        return $this->withConnectionRetry($transport, operationAttempts: self::PUBLISH_RECOVERIES, payload: $payload);
    }

    public function pushRaw($payload, $queue = null, array $options = []): int|string|null
    {
        $this->probeIdleConnection($payload);

        $correlationId = parent::pushRaw($payload, $queue, $options);

        [$destination, $exchange] = $this->publishProperties($queue, $options);
        $this->addQueueToIndex($destination, $exchange);

        return $correlationId;
    }

    /**
     * A delayed message reaches its queue only when the delay expires, so the queue is not indexed here:
     * the scanner indexes it once the message is in it. Without a delay the message goes out through
     * pushRaw(), which indexes the queue.
     */
    public function laterRaw(
        $delay,
        string $payload,
        $queue = null,
        int $attempts = 0,
        ?string $queueType = null,
        bool $confirm = false
    ): int|string|null {
        $this->probeIdleConnection($payload);

        return parent::laterRaw($delay, $payload, $queue, $attempts, $queueType, $confirm);
    }

    protected function publishBatch($jobs, $data = '', $queue = null): void
    {
        // Phase 1: build all AMQPMessages with stable UUIDs - no network I/O here
        $prepared = $this->prepareMessages($jobs, $data, $queue);

        if (! empty($prepared)) {
            $this->probeIdleConnection($prepared[0][0]->getBody());
        }

        // Phase 2: declare + publish as a single retried unit. The same pre-built
        // messages (and their message_id / deduplication keys) are reused on every
        // reconnect retry; declaration is idempotent and re-run after a reconnect.
        $plain = array_values(array_filter($prepared, fn (array $built): bool => ! $built[6]));
        $confirmed = array_values(array_filter($prepared, fn (array $built): bool => $built[6]));

        $this->withConnectionRetry(function () use ($prepared, $plain, $confirmed): void {
            foreach ($prepared as [, $exchange, $destination, $exchangeType, $queueType]) {
                $this->declareDestination($destination, $exchange, $exchangeType, $queueType);
            }

            // Every channel the batch needs is opened before anything is buffered: a failure then leaves no
            // message buffered on a channel, where the next batch on it would publish it again.
            $plainChannel = empty($plain) ? null : $this->getChannel(false, false);
            $confirmChannel = empty($confirmed) ? null : $this->getChannel(false, true);
            // A channel that went stale meanwhile means the connection changed under the declarations made above:
            // the unit recovers and runs again, declarations included.
            if (($plainChannel !== null && $this->isStale($plainChannel))
                || ($confirmChannel !== null && $this->isStale($confirmChannel))
            ) {
                throw new AMQPChannelClosedException('Channel connection is closed.');
            }

            if ($plainChannel !== null) {
                $this->publishBuffered($plainChannel, $plain);
            }
            if ($confirmChannel !== null) {
                $this->publishBuffered($confirmChannel, $confirmed);
                $this->waitForConfirms($confirmChannel);
            }
        }, operationAttempts: self::PUBLISH_RECOVERIES, payload: empty($prepared) ? null : $prepared[0][0]->getBody());

        // once per queue, not per message
        $destinations = [];
        foreach ($prepared as [, $exchange, $destination]) {
            $destinations[$destination] = $exchange;
        }

        foreach ($destinations as $destination => $exchange) {
            $this->addQueueToIndex((string) $destination, $exchange);
        }
    }

    /**
     * php-amqplib empties a channel's batch only after a successful write, so a failed write leaves the
     * messages buffered on the channel, where the next batch would send them again: they are discarded.
     *
     * A blocked connection (a broker alarm) fails before anything is written or awaited, and the channel stays:
     * a vhost consumer may be reading from it. A write timeout is handled by the unit (the connection is
     * replaced). After another failure on a live connection, a confirm channel is closed (confirms recorded for
     * this batch would never resolve) while a plain channel stays; on a connection that is down, the channel is
     * just no longer used.
     */
    private function publishBuffered(AMQPChannel $channel, array $prepared): void
    {
        try {
            foreach ($prepared as [$message, $exchange, $destination]) {
                $channel->batch_basic_publish($message, $exchange, $destination);
            }
            $channel->publish_batch();
        } catch (\Throwable $exception) {
            (function (): void {
                $this->batch_messages = [];
            })->call($channel);

            if (! $exception instanceof AMQPConnectionBlockedException && ! self::isWriteTimeout($exception)) {
                $connected = (bool) $channel->getConnection()?->isConnected();
                if ($this->confirmChannel === $channel) {
                    $this->confirmChannel = null;
                    if ($connected) {
                        $this->closeQuietly($channel);
                    }
                } elseif (! $connected && $this->channel === $channel) {
                    $this->channel = null;
                }
            }

            throw $exception;
        }
    }

    /**
     * php-amqplib's own write timeout (AMQPTimeoutException::writeTimeout()): part of a frame may be in the
     * stream, so nothing more can be written on that connection. A read timeout leaves the stream intact.
     */
    private static function isWriteTimeout(\Throwable $exception): bool
    {
        return $exception instanceof AMQPTimeoutException
            && str_starts_with($exception->getMessage(), 'Error sending data.');
    }

    /**
     * Builds AMQPMessage objects for all jobs without performing any network publish.
     * Separating this from declaration/transport lets reconnect retries reuse the
     * same messages (and therefore the same message_id / deduplication key).
     *
     * @return array<array{0: \PhpAmqpLib\Message\AMQPMessage, 1: string, 2: string, 3: string, 4: string|null, 5: int|string|null, 6: bool}>
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
            $built = $this->buildMessage(
                $this->createPayload($job, $q, $data),
                $q,
                [
                    'queue_type' => ($job instanceof RabbitMQConsumable)
                        ? $job->getQueueType()
                        : RabbitMQConsumable::MQ_TYPE_QUORUM,
                ]
            );

            $built[] = $this->jobRequestsConfirm($job);
            $prepared[] = $built;
        }
        return $prepared;
    }

    protected function createChannel(): AMQPChannel
    {
        return $this->withConnectionRetry(function (): AMQPChannel {
            // A closed connection is not reopened quietly by php-amqplib under a retried unit: the unit's recovery
            // reconnects and runs the whole unit again, declarations included, so nothing is published into a
            // vhost that was recreated empty meanwhile on the strength of declarations made before.
            // A lazy connection (the default) is not connected until its first channel: that first connect is
            // php-amqplib's own, not a lost connection.
            $connection = $this->getConnection();
            if (! $connection->isConnected() && $connection->getServerProperties() !== null) {
                throw new AMQPConnectionClosedException('Connection is closed.');
            }

            return parent::createChannel();
        }, inner: true);
    }

    /**
     * Check a connection that was idle for at least the heartbeat interval before publishing on it.
     *
     * A proxy closes a client that sent no frame for 2 x heartbeat, and php-amqplib notices only after
     * 2 x heartbeat + 1 s, so a publish in between is written into a socket the peer already closed and
     * is lost without an error. A temporary channel round trip either finds the connection alive (and
     * resets the proxy's idle timer) or fails, and only then is the connection re-established. An alive
     * connection is not reconnected: a vhost consumer shares it, and a reconnect would close the channel
     * that holds its unacked delivery. Callers run it before declaring anything, so the queues of a vhost
     * recreated here are declared again by the publish that follows.
     */
    private function probeIdleConnection(string $payload): void
    {
        if ($this->replaceConnection) {
            return;
        }

        $connection = $this->getConnection();
        if (! $connection->isConnected() || $connection->getHeartbeat() <= 0) {
            return;
        }

        $idleSeconds = now()->getPreciseTimestamp() / 1_000_000 - $connection->getLastActivity();
        if ($idleSeconds < $connection->getHeartbeat()) {
            return;
        }

        try {
            parent::createChannel()->close();
        } catch (AMQPConnectionClosedException|AMQPChannelClosedException $exception) {
            $this->logger->warning('RabbitMQQueueBatchable.reconnect', [
                'reason' => 'idle_guard',
                'vhost' => $this->vhostName(),
                'idle_seconds' => round($idleSeconds, 1),
                'job' => self::jobName($payload),
                'error' => $exception->getMessage(),
            ]);

            $this->recoverConnection($exception, 2);
        }
    }

    public function push($job, $data = '', $queue = null)
    {
        $isListenerJob = $job instanceof CallQueuedListener;
        $queue = $isListenerJob
            ? $this->getQueueForListenerJob($job, $queue)
            : $this->getQueueForConsumableJob($job, $queue);

        $options = [
            'queue_type' => ($job instanceof RabbitMQConsumable)
                ? $job->getQueueType()
                : RabbitMQConsumable::MQ_TYPE_QUORUM,
        ];

        // Same publish path as RabbitMQQueue::push (postfix + enqueueUsing + pushRaw), but the AMQPMessage is
        // built ONCE up front, so the initial attempt and every reconnect retry publish the same message_id
        // (transport-level deduplication). Declaration + publish are the retried unit (publishTransport()).
        $confirm = $this->jobRequestsConfirm($job);

        $publishQueue = $this->addQueuePostfix($job, $queue);
        $payload = $this->createPayload($job, $this->getQueue($publishQueue), $data);
        [$message, $exchange, $destination, $exchangeType, $queueType, $correlationId] =
            $this->buildMessage($payload, $publishQueue, $options);

        $result = $this->enqueueUsing(
            $job,
            $payload,
            $publishQueue,
            null,
            function () use ($payload, $message, $exchange, $destination, $exchangeType, $queueType, $correlationId, $confirm) {
                $this->probeIdleConnection($payload);

                $publishResult = $this->publishTransport(function () use ($message, $exchange, $destination, $exchangeType, $queueType, $correlationId, $confirm) {
                    $this->declareDestination($destination, $exchange, $exchangeType, $queueType);
                    $this->publishBasic($message, $exchange, $destination, true, confirm: $confirm);

                    return $correlationId;
                }, $payload);

                // here and not after enqueueUsing(): a job dispatched after commit is published only then
                $this->addQueueToIndex($destination, $exchange);

                return $publishResult;
            }
        );

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
     * Index the queue a message was just published to, so workers poll it before the scanner sees it.
     * The message is already published and the scanner indexes the queue anyway, so a failure here is
     * logged and not thrown.
     *
     * @param string $queue
     * @param string $exchange
     * @return bool
     */
    private function addQueueToIndex(string $queue, string $exchange = ''): bool
    {
        // a message published to an exchange lands in the queues bound to it, which are not known here
        if (('' === $queue) || ('' !== $exchange)) {
            return false;
        }

        $dto = new ConnectionNameDto($this->getConnectionName());
        $vhostName = $dto->getVhostName();
        if ((null === $vhostName)
            || !config('queue.connections.' . $dto->getConfigName() . '.immediate_indexation')
        ) {
            return false;
        }

        try {
            $this->internalStorageManager->setConnection($dto->getConfigName());

            try {
                $this->groupsService->setConnection($dto->getConfigName());
                $groups = $this->groupsService->getAllGroupsNames();
            } catch (RabbitVhostsGroupsException $exception) {
                $groups = [];
            }

            $queueApiDto = new QueueApiDto([
                'name' => $queue,
                'vhost' => $vhostName,
            ]);
            $isQueueActivated = $this->internalStorageManager->activateQueue($queueApiDto, $groups);

            $vhostDto = new VhostApiDto([
                'name' => $vhostName,
            ]);
            $isVhostActivated = $this->internalStorageManager->activateVhost($vhostDto, $groups);
        } catch (\Throwable $exception) {
            $this->logger->warning('RabbitMQQueueBatchable.addQueueToIndex.exception', [
                'vhost_name' => $vhostName,
                'queue' => $queue,
                'error' => $exception->getMessage(),
            ]);

            return false;
        }

        return $isQueueActivated && $isVhostActivated;
    }

    /**
     * @param AMQPConnectionClosedException|AMQPChannelClosedException $exception
     * @return bool
     */
    private function isVhostFailedException(AMQPConnectionClosedException|AMQPChannelClosedException $exception): bool
    {
        return VhostFailure::isMissing($exception, (new ConnectionNameDto($this->getConnectionName()))->getVhostName());
    }

    /**
     * Run a transport-only operation, retrying it after a connection/channel failure.
     *
     * The callable must contain only network I/O - message building must happen
     * before this call so the same payload (and UUID) is reused on every attempt.
     *
     * A unit (a publish, or a standalone call such as opening a channel) always retries on its own, also when
     * it starts while another unit runs (e.g. a job dispatched from a timeout signal handler). An inner retry
     * ($inner: opening a channel or a publish that is part of a unit) inside a running unit only runs its
     * operation: the unit's retry recovers and runs the whole unit again.
     *
     * @param callable $operation         Transport-only operation to run.
     * @param int      $reconnectAttempts Total reconnect attempts before giving up,
     *                                    passed through to reconnect() (default 2).
     * @param int      $operationAttempts Number of times to retry the operation after a
     *                                    connection failure (0 = run once, no retry).
     * @param string|null $payload        Job payload, decoded for the reconnect log line only.
     */
    private function withConnectionRetry(
        callable $operation,
        int $reconnectAttempts = 2,
        int $operationAttempts = 2,
        ?string $payload = null,
        bool $inner = false
    ): mixed {
        if ($inner && $this->runningUnits > 0) {
            return $operation();
        }

        $outerPayload = $this->publishingPayload;
        $outerIdleSeconds = $this->publishingIdleSeconds;
        $this->publishingPayload = $payload;
        // Taken before the operation: a write into a closed socket updates the last activity before it fails.
        $this->publishingIdleSeconds = $this->idleSeconds();

        $this->runningUnits++;
        try {
            $this->forgetStaleChannels();
            $vhostRecreated = false;
            if ($this->replaceConnection) {
                $this->logger->warning('RabbitMQQueueBatchable.reconnect', [
                    'reason' => 'write_timeout',
                    'vhost' => $this->vhostName(),
                    'idle_seconds' => $this->publishingIdleSeconds,
                    'job' => $this->publishingPayload === null ? null : self::jobName($this->publishingPayload),
                ]);
                $vhostRecreated = $this->recoverConnection(new AMQPConnectionClosedException('Connection is closed.'), $reconnectAttempts);
                $this->replaceConnection = false;
            }
            for ($attempt = 0; ; $attempt++) {
                try {
                    return $operation();
                } catch (AMQPTimeoutException $exception) {
                    // A write that timed out may have left part of a frame in the stream: nothing more is written
                    // on this connection, it is replaced before the next unit.
                    if (self::isWriteTimeout($exception)) {
                        $this->channel = null;
                        $this->confirmChannel = null;
                        $this->replaceConnection = true;
                    }

                    throw $exception;
                } catch (AMQPConnectionClosedException|AMQPChannelClosedException $exception) {
                    // No retries left - surface the failure.
                    if ($attempt >= $operationAttempts) {
                        throw $exception;
                    }

                    // Same line as the idle guard, so one alert covers every reconnect before a publish.
                    $this->logger->warning('RabbitMQQueueBatchable.reconnect', [
                        'reason' => 'detected_closed',
                        'vhost' => $this->vhostName(),
                        'idle_seconds' => $this->publishingIdleSeconds,
                        'job' => null === $this->publishingPayload ? null : self::jobName($this->publishingPayload),
                        'attempt' => $attempt + 1,
                        'error' => $exception->getMessage(),
                    ]);

                    // The vhost is recreated at most once per unit: still missing after that, the unit fails.
                    $vhostRecreated = $this->recoverConnection($exception, $reconnectAttempts, ! $vhostRecreated) || $vhostRecreated;
                }
            }
        } finally {
            $this->runningUnits--;
            $this->publishingPayload = $outerPayload;
            $this->publishingIdleSeconds = $outerIdleSeconds;
        }
    }

    private function idleSeconds(): ?float
    {
        $connection = $this->getConnection();

        return $connection->isConnected()
            ? round(now()->getPreciseTimestamp() / 1_000_000 - $connection->getLastActivity(), 1)
            : null;
    }

    private function vhostName(): ?string
    {
        return (new ConnectionNameDto($this->getConnectionName()))->getVhostName();
    }

    private static function jobName(string $payload): ?string
    {
        return json_decode($payload, true)['displayName'] ?? null;
    }

    /**
     * Recover after a connection/channel failure: recreate a missing vhost, then reconnect.
     *
     * A missing vhost shows up at connection.open on a direct broker, and only when a channel is opened behind
     * a proxy such as AMQProxy, possibly after a first failure of another kind. It is recreated at most once
     * here, and not at all when $mayRecreateVhost is false (it was already recreated for this unit); the
     * retried unit then runs again and declares its queues again.
     *
     * @return bool Whether the vhost was recreated.
     *
     * @throws AMQPConnectionClosedException|AMQPChannelClosedException The failure when the vhost cannot be recreated or the reconnect fails.
     */
    private function recoverConnection(
        AMQPConnectionClosedException|AMQPChannelClosedException $exception,
        int $reconnectAttempts,
        bool $mayRecreateVhost = true
    ): bool {
        // Declare everything again after any reconnect: the vhost may have been deleted and recreated empty,
        // also by another process while this one saw only a closed connection, and a publish to a remembered
        // queue that is gone is unroutable and lost. The cost is one passive declare per queue per reconnect.
        $this->forgetDeclarations();

        $vhostRecreated = false;
        while (true) {
            if (! $vhostRecreated && $this->isVhostFailedException($exception)) {
                if (! $mayRecreateVhost || false === $this->createNotExistsVhost()) {
                    throw $exception;
                }
                $vhostRecreated = true;
            }

            try {
                $this->reconnect($reconnectAttempts);

                return $vhostRecreated;
            } catch (AMQPConnectionClosedException|AMQPChannelClosedException $reconnectException) {
                if ($vhostRecreated || ! $this->isVhostFailedException($reconnectException)) {
                    throw $reconnectException;
                }
                $exception = $reconnectException;
            }
        }
    }
}
