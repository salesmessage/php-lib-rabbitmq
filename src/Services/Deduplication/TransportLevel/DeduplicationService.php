<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel;

use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;
use Salesmessage\LibRabbitMQ\Services\DlqDetector;

/**
 * DeduplicationService provides a way to deduplicate messages on the transport level.
 * Application level (idempotency) should be implemented in the Job itself @see RabbitMQConsumable
 *
 * @phpstan-type DeduplicationConfig array{
 *     enabled: bool,
 *     ttl: int,
 *     lock_ttl: int,
 *     action_on_duplication: 'ack'|'reject',
 *     action_on_lock: 'ack'|'reject'|'requeue',
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

    private const ACTION_ACK = 'ack';
    private const ACTION_REJECT = 'reject';
    private const ACTION_REQUEUE = 'requeue';

    private const DEFAULT_LOCK_TTL = 60;
    private const DEFAULT_TTL = 7200;

    private const MAX_LOCK_TTL = 300;
    private const MAX_TTL = 7 * 24 * 60 * 60;

    public function __construct(
        private DeduplicationStore $store,
        private LoggerInterface $logger,
    ) {}

    /**
     * @param \Closure $handler
     * @param AMQPMessage $message
     * @param string|null $queueName
     * @return bool - return "true" if the handler was run
     * @throws \Throwable
     */
    public function decorateWithDeduplication(\Closure $handler, AMQPMessage $message, ?string $queueName = null): bool
    {
        $messageState = $this->getState($message, $queueName);
        try {
            if ($messageState === DeduplicationService::IN_PROGRESS) {
                $action = $this->applyActionOnLock($message);
                $this->logger->warning('DeduplicationService.message_already_in_progress', [
                    'action' => $action,
                    'message_id' => $message->get_properties()['message_id'] ?? null,
                ]);

                return false;
            }

            if ($messageState === DeduplicationService::PROCESSED) {
                $action = $this->applyActionOnDuplication($message);
                $this->logger->warning('DeduplicationService.message_already_processed', [
                    'action' => $action,
                    'message_id' => $message->get_properties()['message_id'] ?? null,
                ]);

                return false;
            }

            $hasPutAsInProgress = $this->markAsInProgress($message, $queueName);
            if ($hasPutAsInProgress === false) {
                $action = $this->applyActionOnLock($message);
                $this->logger->warning('DeduplicationService.message_already_in_progress.skip', [
                    'action' => $action,
                    'message_id' => $message->get_properties()['message_id'] ?? null,
                ]);

                return false;
            }

            $handler();
        } catch (\Throwable $exception) {
            if ($messageState === null) {
                $this->release($message, $queueName);
            }

            $this->logger->error('DeduplicationService.message_processing_exception', [
                'released_message_id' => $message->get_properties()['message_id'] ?? null,
            ]);

            throw $exception;
        }

        return true;
    }

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

    protected function markAsInProgress(AMQPMessage $message, ?string $queueName = null): bool
    {
        $ttl = (int) ($this->getConfig('lock_ttl') ?: self::DEFAULT_LOCK_TTL);
        if ($ttl <= 0 || $ttl > self::MAX_LOCK_TTL) {
            throw new \InvalidArgumentException(sprintf('Invalid TTL seconds. Should be between 1 and %d', self::MAX_LOCK_TTL));
        }

        return $this->add($message, self::IN_PROGRESS, $ttl, $queueName);
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

    protected function applyActionOnLock(AMQPMessage $message): string
    {
        $action = $this->getConfig('action_on_lock', self::ACTION_REQUEUE);
        if ($action === self::ACTION_REJECT) {
            $message->reject(false);
        } elseif ($action === self::ACTION_ACK) {
            $message->ack();
        } else {
            $message->reject(true);
        }

        return $action;
    }

    protected function applyActionOnDuplication(AMQPMessage $message): string
    {
        $action = $this->getConfig('action_on_duplication', self::ACTION_ACK);
        if ($action === self::ACTION_REJECT) {
            $message->reject(false);
        } else {
            $message->ack();
        }

        return $action;
    }

    protected function isEnabled(): bool
    {
        return (bool) $this->getConfig('enabled', false);
    }

    protected function getConfig(string $key, mixed $default = null): mixed
    {
        $value = config("queue.connections.rabbitmq_vhosts.deduplication.transport.$key");

        return $value !== null ? $value : $default;
    }
}
