<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

use PhpAmqpLib\Message\AMQPMessage;
use Salesmessage\LibRabbitMQ\Services\DlqDetector;

/**
 * @phpstan-type DeduplicationConfig array{
 *     enabled: bool,
 *     skip_for_dlq: bool,
 *     ttl: int,
 *     connection: array{
 *       driver: string,
 *       name: string,
 *       key_prefix: string,
 *     }
 * } - {
 *     "deduplication_ttl": TTL in seconds for the message to be considered as processed,
 *     "processing_lock_ttl": TTL in seconds for the lock to be acquired for the message during processing,
 *     "connection.driver": "Only redis is supported now",
 *     "connection.name": "Connection name from config('database.redis.{connection_name}')",
 * }
 */
class DeduplicationService
{
    private const DEFAULT_TTL = 7200;

    public function __construct(private DeduplicationStore $store)
    {
    }

    /**
     * Returns "true" if the message was not processed previously, and it's successfully been added to the store.
     * Returns "false" if the message was already processed and it's a duplicate.
     *
     * @param AMQPMessage $message
     *
     * @return bool
     */
    public function add(AMQPMessage $message): bool
    {
        if (!$this->isEnabled()) {
            return true;
        }

        $messageId = $this->extractMessageId($message);
        if ($messageId === null) {
            return false;
        }

        if (DlqDetector::isDlqMessage($message)) {
            if ($this->getConfig('skip_for_dlq', false)) {
                return true;
            }

            $messageId = 'dlq:' . $messageId;
        }

        $ttl = (int) ($this->getConfig('ttl') ?: self::DEFAULT_TTL);

        return $this->store->add($messageId, $ttl);
    }

    public function release(AMQPMessage $message): void
    {
        if (!$this->isEnabled()) {
            return;
        }
        $messageId = $this->extractMessageId($message);
        if ($messageId === null) {
            return;
        }

        if (DlqDetector::isDlqMessage($message)) {
            if ($this->getConfig('skip_for_dlq', false)) {
                return;
            }

            $messageId = 'dlq:' . $messageId;
        }

        $this->store->release($messageId);
    }

    protected function extractMessageId(AMQPMessage $message): ?string
    {
        $props = $message->get_properties();
        $id = $props['message_id'] ?? null;
        if (is_string($id) && !empty($id)) {
            return $id;
        }
        return null;
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
