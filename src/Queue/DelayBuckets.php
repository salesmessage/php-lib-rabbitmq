<?php

namespace Salesmessage\LibRabbitMQ\Queue;

/**
 * Quantizes a requested delay into a fixed set of buckets so the number of
 * delay queues stays bounded (one permanent queue per bucket) instead of one
 * per distinct delay value.
 *
 * Delays are always rounded UP, so a message is never delivered earlier than requested.
 */
final class DelayBuckets
{
    /**
     * Bucket sizes in seconds, up to one hour.
     */
    private const BUCKETS_SECONDS = [
        1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 300, 420, 600, 900, 1200, 1800, 2700, 3600,
    ];

    /**
     * Beyond the largest bucket, delays are rounded up to a multiple of this step (seconds).
     */
    private const LONG_DELAY_STEP_SECONDS = 15 * 60;

    /**
     * Returns the bucket (in milliseconds) a delay of $ttlMs falls into.
     */
    public static function roundUpMs(int $ttlMs): int
    {
        $seconds = (int) ceil($ttlMs / 1000);

        foreach (self::BUCKETS_SECONDS as $bucket) {
            if ($seconds <= $bucket) {
                return $bucket * 1000;
            }
        }

        return (int) (ceil($seconds / self::LONG_DELAY_STEP_SECONDS) * self::LONG_DELAY_STEP_SECONDS) * 1000;
    }
}
