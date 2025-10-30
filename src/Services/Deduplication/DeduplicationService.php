<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

use PhpAmqpLib\Message\AMQPMessage;
use Salesmessage\LibRabbitMQ\Services\DlqDetector;

/**
 * @phpstan-type DeduplicationConfig array{
 *     enabled: bool,
 *     ttl: int,
 *     lock_ttl: int,
 *     connection: array{
 *       driver: string,
 *       name: string,
 *       key_prefix: string,
 *     }
 * } - {
 *     "ttl": TTL in seconds for the message to be considered as processed,
 *     "lock_ttl": TTL in seconds for the lock to be acquired for the message during in_progress,
 *     "connection.driver": "Only redis is supported now",
 *     "connection.name": "Connection name from config('database.redis.{connection_name}')",
 * }
 */
class DeduplicationService
{
    public const IN_PROGRESS = 'in_progress';
    public const PROCESSED = 'processed';

    private const DEFAULT_LOCK_TTL = 60;
    private const DEFAULT_TTL = 7200;

    private const MAX_LOCK_TTL = 300;
    private const MAX_TTL = 7 * 24 * 60 * 60;

    public function __construct(private DeduplicationStore $store) {}

    /**
     * @param AMQPMessage $message
     * @return string|null - @enum {self::IN_PROGRESS, self::PROCESSED}
     */
    public function getState(AMQPMessage $message, ?string $queueName = null): ?string
    {
        if (!$this->isEnabled()) {
            return null;
        }
        $messageId = $this->getMessageId($message, $queueName);
        if ($messageId === null) {
            return null;
        }

        return $this->store->get($messageId);
    }

    public function markAsInProgress(AMQPMessage $message, ?string $queueName = null): bool
    {
        $ttl = (int) ($this->getConfig('lock_ttl') ?: self::DEFAULT_LOCK_TTL);
        if ($ttl <= 0 || $ttl > self::MAX_LOCK_TTL) {
            throw new \InvalidArgumentException(sprintf('Invalid TTL seconds. Should be between 1 and %d', self::MAX_LOCK_TTL));
        }

        return $this->add($message, self::IN_PROGRESS, $ttl, $queueName);
    }

    public function markAsProcessed(AMQPMessage $message, ?string $queueName = null): bool
    {
        $ttl = (int) ($this->getConfig('ttl') ?: self::DEFAULT_TTL);
        if ($ttl <= 0 || $ttl > self::MAX_TTL) {
            throw new \InvalidArgumentException(sprintf('Invalid TTL seconds. Should be between 1 sec and %d sec', self::MAX_TTL));
        }

        return $this->add($message, self::PROCESSED, $ttl, $queueName);
    }

    public function release(AMQPMessage $message, ?string $queueName = null): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $messageId = $this->getMessageId($message, $queueName);
        if ($messageId === null) {
            return;
        }

        $this->store->release($messageId);
    }

    /**
     *  Returns "true" if the message was not processed previously, and it's successfully been added to the store.
     *  Returns "false" if the message was already processed and it's a duplicate.
     *
     * @param AMQPMessage $message
     * @param string $value
     * @param int $ttl
     * @return bool
     */
    protected function add(AMQPMessage $message, string $value, int $ttl, ?string $queueName = null): bool
    {
        if (!$this->isEnabled()) {
            return true;
        }

        $messageId = $this->getMessageId($message, $queueName);
        if ($messageId === null) {
            return true;
        }

        return $this->store->set($messageId, $value, $ttl, $value === self::PROCESSED);
    }

    protected function getMessageId(AMQPMessage $message, ?string $queueName = null): ?string
    {
        $props = $message->get_properties();
        $messageId = $props['message_id'] ?? null;
        if (!is_string($messageId) || empty($messageId)) {
            return null;
        }

        if (DlqDetector::isDlqMessage($message)) {
            $messageId = 'dlq:' . $messageId;
        }

        if (is_string($queueName) && $queueName !== '') {
            $messageId = $queueName . ':' . $messageId;
        }

        return $messageId;
    }

    protected function isEnabled(): bool
    {
        return (bool) $this->getConfig('enabled', false);
    }

    protected function getConfig(string $key, mixed $default = null): mixed
    {
        $value = config("queue.connections.rabbitmq_vhosts.deduplication.$key");

        return $value !== null ? $value : $default;
    }
}
