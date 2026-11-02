<?php

/** @noinspection PhpRedundantCatchClauseInspection */

namespace Salesmessage\LibRabbitMQ\Queue;

use ErrorException;
use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Events\CallQueuedListener;
use Illuminate\Queue\Queue;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Ramsey\Uuid\Uuid;
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQQueueContract;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob;
use RuntimeException;
use Throwable;

class RabbitMQQueue extends Queue implements QueueContract, RabbitMQQueueContract
{
    /**
     * The RabbitMQ connection instance.
     */
    protected ?AbstractConnection $connection = null;

    /**
     * The RabbitMQ channel instance used for regular (non-confirmed) publishing.
     */
    protected ?AMQPChannel $channel = null;

    /**
     * A separate channel kept in publisher-confirm mode, used only for publishes that
     * request confirms. Kept apart from the plain channel because confirm mode is setup now per job not globally via configs
     */
    protected ?AMQPChannel $confirmChannel = null;

    /**
     * List of already declared exchanges.
     */
    protected array $exchanges = [];

    /**
     * List of already declared queues.
     */
    protected array $queues = [];

    /**
     * List of already bound queues to exchanges.
     */
    protected array $boundQueues = [];

    /**
     * Current job being processed.
     */
    protected ?RabbitMQJob $currentJob = null;

    /**
     * Holds the Configuration
     */
    protected QueueConfig $rabbitMQConfig;

    /**
     * RabbitMQQueue constructor.
     */
    public function __construct(QueueConfig $config)
    {
        $this->rabbitMQConfig = $config;
        $this->dispatchAfterCommit = $config->isDispatchAfterCommit();
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function size($queue = null): int
    {
        $queue = $this->getQueue($queue);

        if (! $this->isQueueExists($queue)) {
            return 0;
        }

        // create a temporary channel, so the main channel will not be closed on exception
        $channel = $this->createChannel();
        [, $size] = $channel->queue_declare($queue, true);
        $channel->close();

        return $size;
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function push($job, $data = '', $queue = null)
    {
        if (false === $this->isJobSupported($job)) {
            throw new \RuntimeException('The job is not supported. RabbitMQQueue.push');
        }

        $options = [
            'queue_type' => ($job instanceof RabbitMQConsumable)
                ? $job->getQueueType()
                : RabbitMQConsumable::MQ_TYPE_QUORUM,
            'confirm' => $this->jobRequestsConfirm($job),
        ];

        $queue = $this->addQueuePostfix($job, $queue);

        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            null,
            function ($payload, $queue) use ($options) {
                return $this->pushRaw($payload, $queue, $options);
            }
        );
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function pushRaw($payload, $queue = null, array $options = []): int|string|null
    {
        $confirm = (bool) ($options['confirm'] ?? false);

        [$message, $exchange, $destination, $exchangeType, $queueType, $correlationId] = $this->buildMessage($payload, $queue, $options);

        return $this->publishTransport(function () use ($message, $exchange, $destination, $exchangeType, $queueType, $correlationId, $confirm) {
            $this->declareDestination($destination, $exchange, $exchangeType, $queueType);
            $this->publishBasic($message, $exchange, $destination, true, confirm: $confirm);

            return $correlationId;
        }, $payload);
    }

    /**
     * Run the network part of a publish: the declarations and the publish itself. The message is built
     * before, so a subclass that retries this part after a reconnect publishes the same message, and
     * declares its queues again (e.g. in a vhost recreated by the recovery).
     */
    protected function publishTransport(callable $transport, string $payload): mixed
    {
        return $transport();
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        if (false === $this->isJobSupported($job)) {
            throw new \RuntimeException('The job is not supported. RabbitMQQueue.later');
        }

        $queueType = ($job instanceof RabbitMQConsumable)
            ? $job->getQueueType()
            : RabbitMQConsumable::MQ_TYPE_QUORUM;

        $confirm = $this->jobRequestsConfirm($job);

        $queue = $this->addQueuePostfix($job, $queue);

        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            $delay,
            function ($payload, $queue, $delay) use ($queueType, $confirm) {
                return $this->laterRaw($delay, $payload, $queue, queueType: $queueType, confirm: $confirm);
            }
        );
    }

    /**
     * @throws AMQPProtocolChannelException
     */
    public function laterRaw(
        $delay,
        string $payload,
        $queue = null,
        int $attempts = 0,
        ?string $queueType = null,
        bool $confirm = false
    ): int|string|null
    {
        $ttl = $this->secondsUntil($delay) * 1000;
        if (null === $queueType) {
            $queueType = RabbitMQConsumable::MQ_TYPE_QUORUM;
        }

        // default options
        $options = [
            'delay' => $delay,
            'attempts' => $attempts,
        ];

        // When no ttl just publish a new message to the exchange or queue
        if ($ttl <= 0) {
            $options['queue_type'] = $queueType;
            $options['confirm'] = $confirm;

            return $this->pushRaw($payload, $queue, $options);
        }

        [$mainDestination, $exchange, $exchangeType, $attempts] = $this->publishProperties($queue, $options);

        // One shared delay queue per TTL in the vhost, for every destination queue.
        $delayName = DelayQueue::name($ttl);

        [$message, $correlationId] = $this->createMessage($payload, $attempts);
        $routingKey = $this->getRoutingKey($this->getQueue($queue));

        return $this->publishTransport(function () use ($mainDestination, $exchange, $exchangeType, $queueType, $delayName, $ttl, $message, $routingKey, $correlationId) {
            // Create a main queue to handle delayed messages
            $this->declareDestination($mainDestination, $exchange, $exchangeType, $queueType);
            $this->declareDelayQueue($delayName, $this->getDelayQueueArguments($ttl));

            // Publish through the delay exchange with the destination routing key: the delay queue has no
            // x-dead-letter-routing-key, so the message is dead-lettered to the destination it was published for.
            $this->publishBasic(
                $message,
                $delayName,
                $routingKey,
                true,
                // Always confirm a delayed publish: it is mandatory, and without confirm mode the plain
                // channel has no return listener, so an unroutable message would be dropped silently.
                confirm: true
            );

            return $correlationId;
        }, $payload);
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function bulk($jobs, $data = '', $queue = null): void
    {
        $this->publishBatch($jobs, $data, $queue);
    }

    /**
     * @throws AMQPProtocolChannelException
     */
    protected function publishBatch($jobs, $data = '', $queue = null): void
    {
        foreach ($jobs as $job) {
            if (false === $this->isJobSupported($job)) {
                throw new \RuntimeException('The job is not supported. RabbitMQQueue.publishBatch');
            }

            $q = $this->addQueuePostfix($job, $queue);

            $this->bulkRaw($this->createPayload($job, $q, $data), $q, [
                'job' => $job,
                'queue_type' => ($job instanceof RabbitMQConsumable)
                    ? $job->getQueueType()
                    : RabbitMQConsumable::MQ_TYPE_QUORUM,
                'confirm' => $this->jobRequestsConfirm($job),
            ]);
        }

        $this->batchPublish();
    }

    /**
     * @throws AMQPProtocolChannelException
     */
    public function bulkRaw(string $payload, ?string $queue = null, array $options = []): int|string|null
    {
        $confirm = (bool) ($options['confirm'] ?? false);

        [$message, $exchange, $destination, $exchangeType, $queueType, $correlationId] = $this->buildMessage($payload, $queue, $options);

        $this->declareDestination($destination, $exchange, $exchangeType, $queueType);

        $this->getChannel(false, $confirm)->batch_basic_publish($message, $exchange, $destination);

        return $correlationId;
    }

    /**
     * Build the AMQP message and resolve its routing for a publish, WITHOUT any
     * network I/O - no destination declaration and no publish happen here.
     * Separating message construction from declaration/transport lets callers build
     * a message once and reuse it - together with its message_id / deduplication key -
     * across reconnect retries of the declaration/publish step.
     *
     * @return array{0: AMQPMessage, 1: string, 2: string, 3: string, 4: string|null, 5: int|string|null}
     */
    protected function buildMessage(string $payload, ?string $queue = null, array $options = []): array
    {
        $queueType = $options['queue_type'] ?? RabbitMQConsumable::MQ_TYPE_QUORUM;
        if (array_key_exists('queue_type', $options)) {
            unset($options['queue_type']);
        }

        [$destination, $exchange, $exchangeType, $attempts] = $this->publishProperties($queue, $options);

        [$message, $correlationId] = $this->createMessage($payload, $attempts);

        return [$message, $exchange, $destination, $exchangeType, $queueType, $correlationId];
    }

    /**
     * {@inheritdoc}
     *
     * @throws Throwable
     */
    public function pop($queue = null)
    {
        try {
            $queue = $this->getQueue($queue);

            $job = $this->getJobClass();

            /** @var AMQPMessage|null $message */
            if ($message = $this->getChannel()->basic_get($queue)) {
                return $this->currentJob = new $job(
                    $this->container,
                    $this,
                    $message,
                    $this->connectionName,
                    $queue
                );
            }
        } catch (AMQPProtocolChannelException $exception) {
            // If there is no exchange or queue AMQP will throw exception with code 404
            // We need to catch it and return null
            if ($exception->amqp_reply_code === 404) {
                // Because of the channel exception the channel was closed and removed.
                // We have to open a new channel. Because else the worker(s) are stuck in a loop, without processing.
                try {
                    $this->getChannel(true);
                } catch (AMQPChannelClosedException|AMQPConnectionClosedException $closed) {
                    throw $this->lostConnection($closed);
                }

                return null;
            }

            throw $exception;
        } catch (AMQPChannelClosedException|AMQPConnectionClosedException $exception) {
            throw $this->lostConnection($exception);
        }

        return null;
    }

    /**
     * Queue::pop is used by the worker to receive a new job. The thrown exception is checked by
     * Illuminate\Database\DetectsLostConnections::causedByLostConnection: it has to contain one of
     * several phrases in its message for the worker to restart, otherwise the worker keeps working
     * with a broken connection.
     */
    private function lostConnection(AMQPChannelClosedException|AMQPConnectionClosedException $exception): AMQPRuntimeException
    {
        return new AMQPRuntimeException(
            'Lost connection: '.$exception->getMessage(),
            $exception->getCode(),
            $exception
        );
    }

    /**
     * @throws RuntimeException
     */
    public function getConnection(): AbstractConnection
    {
        if (! $this->connection) {
            throw new RuntimeException('Queue has no AMQPConnection set.');
        }

        return $this->connection;
    }

    public function setConnection(AbstractConnection $connection): RabbitMQQueue
    {
        $this->connection = $connection;

        return $this;
    }

    /**
     * Job class to use.
     *
     *
     * @throws Throwable
     */
    public function getJobClass(): string
    {
        $job = $this->getRabbitMQConfig()->getAbstractJob();

        throw_if(
            ! is_a($job, RabbitMQJob::class, true),
            Exception::class,
            sprintf('Class %s must extend: %s', $job, RabbitMQJob::class)
        );

        return $job;
    }

    /**
     * Gets a queue/destination, by default the queue option set on the connection.
     */
    public function getQueue($queue = null): string
    {
        return $queue ?: $this->getRabbitMQConfig()->getQueue();
    }

    /**
     * Checks if the given exchange already present/defined in RabbitMQ.
     * Returns false when the exchange is missing.
     *
     *
     * @throws AMQPProtocolChannelException
     */
    public function isExchangeExists(string $exchange): bool
    {
        if ($this->isExchangeDeclared($exchange)) {
            return true;
        }

        try {
            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->createChannel();
            $channel->exchange_declare($exchange, '', true);
            $channel->close();

            $this->exchanges[] = $exchange;

            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * Declare an exchange in rabbitMQ, when not already declared.
     */
    public function declareExchange(
        string $name,
        string $type = AMQPExchangeType::DIRECT,
        bool $durable = true,
        bool $autoDelete = false,
        array $arguments = []
    ): void {
        if ($this->isExchangeDeclared($name)) {
            return;
        }

        $this->getChannel()->exchange_declare(
            $name,
            $type,
            false,
            $durable,
            $autoDelete,
            false,
            true,
            new AMQPTable($arguments)
        );
    }

    /**
     * Delete an exchange from rabbitMQ, only when present in RabbitMQ.
     *
     *
     * @throws AMQPProtocolChannelException
     */
    public function deleteExchange(string $name, bool $unused = false): void
    {
        if (! $this->isExchangeExists($name)) {
            return;
        }

        $idx = array_search($name, $this->exchanges);
        unset($this->exchanges[$idx]);

        $this->getChannel()->exchange_delete(
            $name,
            $unused
        );
    }

    /**
     * Checks if the given queue already present/defined in RabbitMQ.
     * Returns false when the queue is missing.
     *
     *
     * @throws AMQPProtocolChannelException
     */
    public function isQueueExists(?string $name = null): bool
    {
        $queueName = $this->getQueue($name);

        if ($this->isQueueDeclared($queueName)) {
            return true;
        }

        try {
            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->createChannel();
            $channel->queue_declare($queueName, true);
            $channel->close();

            $this->queues[] = $queueName;

            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * Declare a queue in rabbitMQ, when not already declared.
     */
    public function declareQueue(
        string $name,
        bool $durable = true,
        bool $autoDelete = false,
        array $arguments = []
    ): void {
        if ($this->isQueueDeclared($name)) {
            return;
        }

        $this->getChannel()->queue_declare(
            $name,
            false,
            $durable,
            false,
            $autoDelete,
            false,
            new AMQPTable($arguments)
        );
    }

    /**
     * Declare a shared delay queue with its fanout exchange and the binding between them, always
     * redeclaring (never cached like declareQueue()): the queue is permanent, but it can still be
     * removed externally (cleanup commands, vhost removal), and a long-lived consumer must be able
     * to recreate it on the next delayed dispatch instead of publishing into a missing queue.
     *
     * Also tolerates a 406 PRECONDITION_FAILED reply: two pods can declare the same delay queue with
     * different arguments while a rolling deploy overlaps. The queue already exists and is still
     * usable, so the mismatch is not fatal -- throwing here would drop the message being published
     * right after.
     *
     * @throws AMQPProtocolChannelException
     */
    protected function declareDelayQueue(string $name, array $arguments): void
    {
        $steps = [
            fn (AMQPChannel $channel) => $channel->exchange_declare($name, AMQPExchangeType::FANOUT, false, true, false),
            fn (AMQPChannel $channel) => $channel->queue_declare($name, false, true, false, false, false, new AMQPTable($arguments)),
            fn (AMQPChannel $channel) => $channel->queue_bind($name, $name),
        ];

        // A dedicated, throwaway channel, so the main channel used for
        // publishing is not closed by the broker on a protocol exception.
        $channel = $this->createChannel();

        foreach ($steps as $step) {
            try {
                $step($channel);
            } catch (AMQPProtocolChannelException $exception) {
                if (406 !== $exception->amqp_reply_code) {
                    throw $exception;
                }

                // The broker closed the channel; the entity exists, so go on with the next step on a new one.
                $channel = $this->createChannel();
            }
        }

        $channel->close();
    }

    /**
     * Delete a queue from rabbitMQ, only when present in RabbitMQ.
     *
     *
     * @throws AMQPProtocolChannelException
     */
    public function deleteQueue(string $name, bool $if_unused = false, bool $if_empty = false): void
    {
        if (! $this->isQueueExists($name)) {
            return;
        }

        $idx = array_search($name, $this->queues);
        unset($this->queues[$idx]);

        $this->getChannel()->queue_delete($name, $if_unused, $if_empty);
    }

    /**
     * Bind a queue to an exchange.
     */
    public function bindQueue(string $queue, string $exchange, string $routingKey = ''): void
    {
        if (in_array(
            implode('', compact('queue', 'exchange', 'routingKey')),
            $this->boundQueues,
            true
        )) {
            return;
        }

        $this->getChannel()->queue_bind($queue, $exchange, $routingKey);
    }

    /**
     * Purge the queue of messages.
     */
    public function purge(?string $queue = null): void
    {
        // create a temporary channel, so the main channel will not be closed on exception
        $channel = $this->createChannel();
        $channel->queue_purge($this->getQueue($queue));
        $channel->close();
    }

    /**
     * Acknowledge the message.
     */
    public function ack(RabbitMQJob $job): void
    {
        $message = $job->getRabbitMQMessage();
        $this->deliveryChannel($message)->basic_ack($message->getDeliveryTag());
    }

    /**
     * Reject the message.
     */
    public function reject(RabbitMQJob $job, bool $requeue = false): void
    {
        $message = $job->getRabbitMQMessage();
        $this->deliveryChannel($message)->basic_reject($message->getDeliveryTag(), $requeue);
    }

    /**
     * A delivery tag is valid only on the channel that delivered the message: after a reconnect the
     * queue's channel is another one, and the broker closes it on an unknown delivery tag.
     */
    private function deliveryChannel(AMQPMessage $message): AMQPChannel
    {
        $channel = $message->getChannel();
        if ($channel === null) {
            return $this->getChannel();
        }

        if ($this->isStale($channel)) {
            // Frames go out by channel id: a live channel under the same id on the reconnected connection is
            // where this delivery tag goes (and where a vhost consumer that kept the old object received it).
            // Without one, writing the id would make the broker close the whole connection.
            $live = $channel->getConnection()->channels[$channel->getChannelId()] ?? null;
            if (! $live instanceof AMQPChannel) {
                throw new AMQPChannelClosedException('Channel connection is closed.');
            }

            return $live;
        }

        return $channel;
    }

    /**
     * A connection that reconnects on its own (php-amqplib does when it is used after a missed heartbeat or a
     * failed write) empties its channel list without closing the old channel objects: one of them would write
     * on the new connection with a channel id it doesn't own, and the broker drops the frame or closes the
     * connection. A channel closed the normal way has no connection and fails by itself when used.
     */
    protected function isStale(AMQPChannel $channel): bool
    {
        $connection = $channel->getConnection();

        return $connection !== null && ($connection->channels[$channel->getChannelId()] ?? null) !== $channel;
    }

    /**
     * A cached channel is used again unless it is gone: missing, closed by the broker (a channel-level error),
     * or stale. A stale channel means the connection reconnected underneath, possibly into a vhost recreated
     * empty, so the declarations made before are forgotten too.
     */
    private function isUsable(?AMQPChannel $channel): bool
    {
        if ($channel === null || $channel->is_open() === false) {
            return false;
        }
        if ($this->isStale($channel)) {
            $this->forgetDeclarations();

            return false;
        }

        return true;
    }

    /**
     * Drop the cached channels a connection that reconnected on its own left behind, with the declarations
     * made on them, before a retried unit declares and publishes.
     */
    protected function forgetStaleChannels(): void
    {
        foreach (['channel', 'confirmChannel'] as $property) {
            if ($this->{$property} !== null && $this->isStale($this->{$property})) {
                $this->{$property} = null;
                $this->forgetDeclarations();
            }
        }
    }

    protected function forgetDeclarations(): void
    {
        $this->queues = [];
        $this->exchanges = [];
        $this->boundQueues = [];
    }

    /**
     * Create a AMQP message.
     */
    protected function createMessage($payload, int $attempts = 0): array
    {
        $properties = [
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];

        $currentPayload = json_decode($payload, true);
        if ($correlationId = $currentPayload['id'] ?? null) {
            $properties['correlation_id'] = $correlationId;
            $properties['message_id'] = Uuid::uuid7()->toString();
        }

        if ($this->getRabbitMQConfig()->isPrioritizeDelayed()) {
            $properties['priority'] = $attempts;
        }

        if (isset($currentPayload['priority'])) {
            $properties['priority'] = $currentPayload['priority'];
        }

        $message = new AMQPMessage($payload, $properties);

        $message->set('application_headers', new AMQPTable([
            'laravel' => [
                'attempts' => $attempts,
            ],
        ]));

        return [
            $message,
            $correlationId,
        ];
    }

    /**
     * Create a payload array from the given job and data.
     *
     * @param  string|object  $job
     * @param  string  $queue
     * @param  mixed  $data
     */
    protected function createPayloadArray($job, $queue, $data = ''): array
    {
        $payload = array_merge(parent::createPayloadArray($job, $queue, $data), [
            'id' => $this->getRandomId(),
        ]);

        if (is_object($job) && isset($job->priority)) {
            $payload['priority'] = $job->priority;
        }

        return $payload;
    }

    /**
     * Get a random ID string.
     */
    protected function getRandomId(): string
    {
        return Str::uuid();
    }

    /**
     * Close the connection to RabbitMQ.
     *
     *
     * @throws Exception
     */
    public function close(): void
    {
        if (isset($this->currentJob) && ! $this->currentJob->isDeletedOrReleased()) {
            try {
                $this->reject($this->currentJob, true);
            } catch (AMQPConnectionClosedException|AMQPChannelClosedException|ErrorException) {
                // Connection or channel already closed, ignore
            }
        }

        try {
            $connection = $this->getConnection();
            if ($connection->isConnected()) {
                $connection->close();
            }
        } catch (Throwable) {
            // This is expected when the connection is already closed or broken
        }
    }

    /**
     * Get the Queue arguments.
     */
    protected function getQueueArguments(string $destination, ?string $queueType = null): array
    {
        $isQuorum = $this->getRabbitMQConfig()->isQuorum() || ($queueType === RabbitMQConsumable::MQ_TYPE_QUORUM);
        $arguments = [];

        // Messages without a priority property are treated as if their priority were 0.
        // Messages with a priority which is higher than the queue's maximum, are treated as if they were
        // published with the maximum priority.
        // Quorum queues does not support priority.
        if ($this->getRabbitMQConfig()->isPrioritizeDelayed() && ! $this->getRabbitMQConfig()->isQuorum()) {
            $arguments['x-max-priority'] = $this->getRabbitMQConfig()->getQueueMaxPriority();
        }

        if ($this->getRabbitMQConfig()->isRerouteFailed()) {
            $arguments['x-dead-letter-exchange'] = $this->getFailedExchange();
            $arguments['x-dead-letter-routing-key'] = $this->getFailedRoutingKey($destination);
        }

        if ($isQuorum) {
            $arguments['x-queue-type'] = 'quorum';

            // optional: initial group size for quorum queues
            $initialGroupSize = $this->getRabbitMQConfig()->getQuorumInitialGroupSize();
            if ($initialGroupSize !== null) {
                $arguments['x-quorum-initial-group-size'] = $initialGroupSize;
            }
        }

        return $arguments;
    }

    /**
     * Get the Delay queue arguments.
     */
    protected function getDelayQueueArguments(int $ttl): array
    {
        return DelayQueue::arguments($ttl, $this->getExchange());
    }

    /**
     * Get the exchange name, or empty string; as default value.
     */
    protected function getExchange(?string $exchange = null): string
    {
        return $exchange ?? $this->getRabbitMQConfig()->getExchange();
    }

    /**
     * Get the routing-key for when you use exchanges
     * The default routing-key is the given destination.
     */
    protected function getRoutingKey(string $destination): string
    {
        return ltrim(sprintf($this->getRabbitMQConfig()->getExchangeRoutingKey(), $destination), '.');
    }

    /**
     * Get the exchangeType, or AMQPExchangeType::DIRECT as default.
     */
    protected function getExchangeType(?string $type = null): string
    {
        $constant = AMQPExchangeType::class.'::'.Str::upper($type ?: $this->getRabbitMQConfig()->getExchangeType());

        return defined($constant) ? constant($constant) : AMQPExchangeType::DIRECT;
    }

    /**
     * Get the exchange for failed messages.
     */
    protected function getFailedExchange(?string $exchange = null): string
    {
        return $exchange ?? $this->getRabbitMQConfig()->getFailedExchange();
    }

    /**
     * Get the routing-key for failed messages
     * The default routing-key is the given destination substituted by '.failed'.
     */
    protected function getFailedRoutingKey(string $destination): string
    {
        return ltrim(sprintf($this->getRabbitMQConfig()->getFailedRoutingKey(), $destination), '.');
    }

    /**
     * Checks if the exchange was already declared.
     */
    protected function isExchangeDeclared(string $name): bool
    {
        return in_array($name, $this->exchanges, true);
    }

    /**
     * Checks if the queue was already declared.
     */
    protected function isQueueDeclared(string $name): bool
    {
        return in_array($name, $this->queues, true);
    }

    /**
     * Declare the destination when necessary.
     *
     * @throws AMQPProtocolChannelException
     */
    protected function declareDestination(
        string $destination,
        ?string $exchange = null,
        string $exchangeType = AMQPExchangeType::DIRECT,
        ?string $queueType = null,
    ): void {
        // When an exchange is provided and no exchange is present in RabbitMQ, create an exchange.
        if ($exchange && ! $this->isExchangeExists($exchange)) {
            $this->declareExchange($exchange, $exchangeType);
        }

        // When an exchange is provided, just return.
        if ($exchange) {
            return;
        }

        // When the queue already exists, just return.
        if ($this->isQueueExists($destination)) {
            return;
        }

        // Create a queue for amq.direct publishing.
        $this->declareQueue(
            $destination,
            true,
            false,
            $this->getQueueArguments($destination, $queueType)
        );
    }

    /**
     * Determine all publish properties.
     */
    protected function publishProperties($queue, array $options = []): array
    {
        $queue = $this->getQueue($queue);
        $attempts = Arr::get($options, 'attempts') ?: 0;

        $destination = $this->getRoutingKey($queue);
        $exchange = $this->getExchange(Arr::get($options, 'exchange'));
        $exchangeType = $this->getExchangeType(Arr::get($options, 'exchange_type'));

        return [$destination, $exchange, $exchangeType, $attempts];
    }

    protected function getRabbitMQConfig(): QueueConfig
    {
        return $this->rabbitMQConfig;
    }

    /**
     * @param $job
     * @return bool
     */
    protected function isJobSupported($job): bool
    {
        if ($job instanceof CallQueuedListener) {
            return true;
        }

        if ($job instanceof RabbitMQConsumable) {
            return true;
        }

        return false;
    }

    /**
     * @param $job
     * @param $queue
     * @return mixed|string|null
     */
    protected function addQueuePostfix($job, $queue = null)
    {
        if ((null === $queue) || ('' === $queue)) {
            return $queue;
        }

        $queueType = ($job instanceof RabbitMQConsumable)
            ? $job->getQueueType()
            : RabbitMQConsumable::MQ_TYPE_QUORUM;

        $queuePostfix = (RabbitMQConsumable::MQ_TYPE_QUORUM === $queueType)
            ? $this->getRabbitMQConfig()->getQuorumQueuePostfix()
            : '';
        if (('' === $queuePostfix) || str_ends_with($queue, $queuePostfix)) {
            return $queue;
        }

        $queue .= $queuePostfix;

        return $queue;
    }

    /**
     * @throws AMQPChannelClosedException
     * @throws AMQPConnectionClosedException
     * @throws AMQPConnectionBlockedException
     */
    protected function publishBasic(
        $msg,
        $exchange = '',
        $destination = '',
        $mandatory = false,
        $immediate = false,
        $ticket = null,
        bool $confirm = false
    ): void
    {
        $channel = $this->getChannel(false, $confirm);
        $channel->basic_publish($msg, $exchange, $destination, $mandatory, $immediate, $ticket);

        if ($confirm) {
            $this->waitForConfirms($channel);
        }
    }

    /**
     * Flush both channel batch buffers. Messages were buffered onto the plain or the
     * confirm channel by bulkRaw() depending on their confirm flag; publish_batch() is a
     * no-op on a channel with an empty buffer. Only the confirm channel is waited on.
     */
    protected function batchPublish(): void
    {
        if ($this->channel !== null) {
            $this->channel->publish_batch();
        }

        if ($this->confirmChannel !== null) {
            $this->confirmChannel->publish_batch();
            $this->waitForConfirms($this->confirmChannel);
        }
    }

    /**
     * Return the publishing channel. When $confirm is true a dedicated channel kept in
     * publisher-confirm mode is returned; otherwise the plain channel. Both are created
     * lazily and recreated when $forceNew is set.
     */
    public function getChannel($forceNew = false, bool $confirm = false): AMQPChannel
    {
        if ($confirm) {
            if ($forceNew || ! $this->isUsable($this->confirmChannel)) {
                // Kept only once in confirm mode: after a failed confirm_select a publish on it would
                // never be confirmed.
                $confirmChannel = $this->createChannel();
                try {
                    $this->enablePublisherConfirms($confirmChannel);
                } catch (Throwable $exception) {
                    $this->closeQuietly($confirmChannel);

                    throw $exception;
                }
                $this->confirmChannel = $confirmChannel;
            }

            return $this->confirmChannel;
        }

        if ($forceNew || ! $this->isUsable($this->channel)) {
            $this->channel = $this->createChannel();
        }

        return $this->channel;
    }

    /**
     * Put the given channel into publisher-confirm mode. A broker nack or an unroutable
     * returned message is turned into an exception so an unconfirmed publish fails loudly.
     */
    protected function enablePublisherConfirms(AMQPChannel $channel): void
    {
        $channel->confirm_select();

        $channel->set_nack_handler(static function (AMQPMessage $message): void {
            throw new AMQPRuntimeException(
                'RabbitMQ publisher confirm: message was nacked by the broker.'
            );
        });

        $channel->set_return_listener(static function (
            int $replyCode,
            string $replyText,
            string $exchange,
            string $routingKey,
            AMQPMessage $message
        ): void {
            throw new AMQPRuntimeException(sprintf(
                'RabbitMQ publisher confirm: message returned as unroutable (%d %s), exchange "%s", routing key "%s".',
                $replyCode,
                $replyText,
                $exchange,
                $routingKey
            ));
        });
    }

    /**
     * Block until the broker confirms every message published on the given channel.
     *
     * @throws \PhpAmqpLib\Exception\AMQPTimeoutException When no confirm arrives within the configured timeout.
     * @throws AMQPRuntimeException                       When the broker nacks or returns a message.
     */
    protected function waitForConfirms(AMQPChannel $channel): void
    {
        $channel->wait_for_pending_acks_returns(
            $this->getRabbitMQConfig()->getPublisherConfirmTimeout()
        );
    }

    protected function jobRequestsConfirm($job): bool
    {
        return $job instanceof RabbitMQConsumable && $job->shouldConfirmOnPublish();
    }

    protected function createChannel(): AMQPChannel
    {
        return $this->getConnection()->channel();
    }

    /**
     * Reconnect using the original connection settings, retrying up to $attempts
     * times when the reconnect itself fails with a connection/channel error.
     *
     * No channel is opened here: the old ones are dropped, and the next use opens new ones through
     * getChannel(), inside the caller's own retry. Opening them here would go through createChannel(),
     * which a subclass may wrap in a retry that reconnects: that recursed without bound when the reconnect
     * succeeded but opening a channel kept failing (a proxy such as AMQProxy accepting the connection while
     * the broker behind it is down). A channel opened here would also leak when the caller opens its own.
     *
     * @param int $attempts Total reconnect attempts before giving up (default 1).
     *
     * @throws Exception
     */
    protected function reconnect(int $attempts = 1): void
    {
        $this->channel = null;
        $this->confirmChannel = null;

        $attempt = 1;
        while (true) {
            try {
                // Reconnects using the original connection settings.
                $this->getConnection()->reconnect();

                return;
            } catch (AMQPConnectionClosedException|AMQPChannelClosedException $exception) {
                if ($attempt >= $attempts) {
                    throw $exception;
                }
                $attempt++;
            }
        }
    }

    protected function closeQuietly(AMQPChannel $channel): void
    {
        try {
            $channel->close();
        } catch (Throwable) {
            // The channel is dropped either way.
        }
    }
}
