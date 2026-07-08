<?php

namespace Salesmessage\LibRabbitMQ\Queue\Jobs;

use Illuminate\Container\Container;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Encryption\Encrypter;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Support\Arr;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;
use Salesmessage\LibRabbitMQ\Horizon\RabbitMQQueue as HorizonRabbitMQQueue;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;

class RabbitMQJob extends Job implements JobContract
{
    /**
     * The RabbitMQ queue instance.
     *
     * @var RabbitMQQueue
     */
    protected $rabbitmq;

    /**
     * The RabbitMQ message instance.
     *
     * @var AMQPMessage
     */
    protected $message;

    /**
     * The JSON decoded version of "$message".
     *
     * @var array
     */
    protected $decoded;

    /**
     * Memoized, unserialized command for this message.
     *
     * The command is unserialized more than once per message (e.g. the deduplication
     * check and then the actual execution). Each unserialize restores every Eloquent
     * model carried by the job via SerializesModels, which is a DB query per model.
     * Caching the restored command keeps that to a single restoration per message.
     *
     * @var object|null
     */
    protected ?object $payloadData = null;

    public function __construct(
        Container $container,
        RabbitMQQueue $rabbitmq,
        AMQPMessage $message,
        string $connectionName,
        string $queue
    ) {
        $this->container = $container;
        $this->rabbitmq = $rabbitmq;
        $this->message = $message;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
        $this->decoded = $this->payload();
    }

    /**
     * {@inheritdoc}
     */
    public function getJobId()
    {
        return $this->decoded['id'] ?? null;
    }

    /**
     * {@inheritdoc}
     */
    public function getRawBody(): string
    {
        return $this->message->getBody();
    }

    /**
     * {@inheritdoc}
     */
    public function attempts(): int
    {
        if (! $data = $this->getRabbitMQMessageHeaders()) {
            return 1;
        }

        $laravelAttempts = (int) Arr::get($data, 'laravel.attempts', 0);

        return $laravelAttempts + 1;
    }

    /**
     * {@inheritdoc}
     */
    public function markAsFailed(): void
    {
        parent::markAsFailed();

        // We must tel rabbitMQ this Job is failed
        // The message must be rejected when the Job marked as failed, in case rabbitMQ wants to do some extra magic.
        // like: Death lettering the message to an other exchange/routing-key.
        $this->rabbitmq->reject($this);
    }

    /**
     * {@inheritdoc}
     *
     * @throws BindingResolutionException
     */
    public function delete(): void
    {
        parent::delete();

        // When delete is called and the Job was not failed, the message must be acknowledged.
        // This is because this is a controlled call by a developer. So the message was handled correct.
        if (! $this->failed) {
            $this->rabbitmq->ack($this);
        }

        // required for Laravel Horizon
        if ($this->rabbitmq instanceof HorizonRabbitMQQueue) {
            $this->rabbitmq->deleteReserved($this->queue, $this);
        }
    }

    /**
     * Release the job back into the queue.
     *
     * @param  int  $delay
     *
     * @throws AMQPProtocolChannelException
     */
    public function release($delay = 0): void
    {
        parent::release();

        $consumableJob = $this->getPayloadData();
        $queueType = ($consumableJob instanceof RabbitMQConsumable)
            ? $consumableJob->getQueueType()
            : RabbitMQConsumable::MQ_TYPE_QUORUM;

        $confirm = ($consumableJob instanceof RabbitMQConsumable) && $consumableJob->shouldConfirmOnPublish();

        // Always create a new message when this Job is released
        $this->rabbitmq->laterRaw(
            $delay,
            $this->message->getBody(),
            $this->queue,
            $this->attempts(),
            $queueType,
            $confirm
        );

        // Releasing a Job means the message was failed to process.
        // Because this Job message is always recreated and pushed as new message, this Job message is correctly handled.
        // We must tell rabbitMQ this job message can be removed by acknowledging the message.
        $this->rabbitmq->ack($this);
    }

    /**
     * @return object
     * @throws \RuntimeException
     */
    public function getPayloadData(): object
    {
        if (null !== $this->payloadData) {
            return $this->payloadData;
        }

        $payload = $this->payload();

        $data = $payload['data'];

        if (str_starts_with($data['command'], 'O:')) {
            return $this->payloadData = unserialize($data['command']);
        }

        if ($this->container->bound(Encrypter::class)) {
            return $this->payloadData = unserialize($this->container[Encrypter::class]->decrypt($data['command']));
        }

        throw new \RuntimeException('Unable to extract job data.');
    }

    /**
     * Returns target class name
     *
     * @return mixed
     */
    public function getPayloadClass(): string
    {
        $payload = $this->payload();

        return $payload['data']['commandName'];
    }

    /**
     * Get the underlying RabbitMQ connection.
     */
    public function getRabbitMQ(): RabbitMQQueue
    {
        return $this->rabbitmq;
    }

    /**
     * Get the underlying RabbitMQ message.
     */
    public function getRabbitMQMessage(): AMQPMessage
    {
        return $this->message;
    }

    /**
     * Get the headers from the rabbitMQ message.
     */
    protected function getRabbitMQMessageHeaders(): ?array
    {
        /** @var AMQPTable|null $headers */
        if (! $headers = Arr::get($this->message->get_properties(), 'application_headers')) {
            return null;
        }

        return $headers->getNativeData();
    }
}
