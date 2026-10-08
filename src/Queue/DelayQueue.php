<?php

namespace Salesmessage\LibRabbitMQ\Queue;

use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;

/**
 * Topology of the shared delay queues: one permanent quorum queue per delay (TTL) in a vhost,
 * regardless of the destination queue, fed through a fanout exchange of the same name.
 *
 * Delay queues must never have x-expires: on RabbitMQ 4.3.1 the quorum queue expiry is a fixed
 * lifetime from creation (a redeclare or a publish does not reset it), so the broker deletes the
 * queue together with the messages still waiting in it.
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

    /**
     * Name of the per-destination queue used by the transport-level dedup lock requeue.
     * It is not the legacy `<queue>.dedup-lock-delay.<ms>` (that one was declared with x-expires,
     * and redeclaring an existing queue with other arguments is a 406 on the consumer channel).
     */
    public static function lockQueueName(string $baseQueue, int $ttlMs): string
    {
        return $baseQueue.'.dedup-lock-delay-v2.'.$ttlMs;
    }

    /**
     * Dedup lock-delay queue arguments: same shape as before, but permanent (no x-expires).
     */
    public static function lockQueueArguments(int $ttlMs, string $deadLetterExchange, string $deadLetterRoutingKey): array
    {
        return [
            'x-message-ttl' => $ttlMs,
            'x-dead-letter-exchange' => $deadLetterExchange,
            'x-dead-letter-routing-key' => $deadLetterRoutingKey,
            'x-queue-type' => RabbitMQConsumable::MQ_TYPE_QUORUM,
        ];
    }
}
