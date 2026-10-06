<?php

namespace Salesmessage\LibRabbitMQ\Queue;

use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;

/**
 * Topology of the shared delay queues: one quorum queue per delay (TTL) in a vhost, regardless of
 * the destination queue, fed through a fanout exchange of the same name. A queue is deleted by the
 * broker only after it has been idle for 24 h on top of its TTL.
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
     * Idle time after which an unused delay queue is deleted by the broker, on top of its TTL.
     */
    public const IDLE_EXPIRY_MS = 86400000;

    /**
     * Delay queue arguments. x-expires is an idle timer (reset by a consumer, basic.get or redeclare,
     * not by publishing). The queue is redeclared on every delayed publish, so a queue in use is
     * refreshed on each dispatch. It is deleted only IDLE_EXPIRY_MS after the last dispatch with this
     * TTL, when every message in it has already been dead-lettered. There is intentionally no
     * x-dead-letter-routing-key (taken from the message).
     *
     * Any caching of the delay-queue declare must stay much shorter than IDLE_EXPIRY_MS.
     */
    public static function arguments(int $ttlMs, string $deadLetterExchange): array
    {
        return [
            'x-dead-letter-exchange' => $deadLetterExchange,
            'x-expires' => $ttlMs + self::IDLE_EXPIRY_MS,
            'x-message-ttl' => $ttlMs,
            'x-queue-type' => RabbitMQConsumable::MQ_TYPE_QUORUM,
        ];
    }
}
