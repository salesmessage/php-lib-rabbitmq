<?php

namespace Salesmessage\LibRabbitMQ\Queue;

use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;

/**
 * Topology of the shared delay queues: one permanent quorum queue per delay (TTL) in a vhost,
 * regardless of the destination queue, fed through a fanout exchange of the same name.
 *
 * The destination is not stored on the queue: a message is published to the fanout exchange with the
 * destination routing key, and the queue has no x-dead-letter-routing-key, so when the TTL expires
 * RabbitMQ dead-letters it with its original routing key, i.e. to the right destination queue.
 */
final class DelayQueue
{
    /**
     * Name of both the delay queue and its fanout exchange for the given delay.
     */
    public static function name(int $ttlMs): string
    {
        return 'delay.'.$ttlMs;
    }

    /**
     * Delay queue arguments. There is intentionally no x-expires (the queue is never auto-deleted)
     * and no x-dead-letter-routing-key (taken from the message).
     */
    public static function arguments(int $ttlMs, string $deadLetterExchange): array
    {
        return [
            'x-dead-letter-exchange' => $deadLetterExchange,
            'x-message-ttl' => $ttlMs,
            'x-queue-type' => RabbitMQConsumable::MQ_TYPE_QUORUM,
        ];
    }
}
