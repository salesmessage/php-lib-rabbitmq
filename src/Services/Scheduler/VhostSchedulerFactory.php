<?php

namespace Salesmessage\LibRabbitMQ\Services\Scheduler;

use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;

class VhostSchedulerFactory
{
    public const TYPE_LAST_PROCESSING_BASED = 'last_processing_based';

    public const TYPE_TIME_SPENT_BASED = 'time_spent_based';

    /**
     * Build a scheduler from a type string and its options.
     * Unknown types fall back to the recency-based scheduler.
     *
     * @param InternalStorageManager $storage
     * @param string $type
     * @param array $options options for time_spent_based: window, bucket, reservation_estimate
     * @return VhostSchedulerInterface
     */
    public static function make(
        InternalStorageManager $storage,
        string $type,
        array $options = []
    ): VhostSchedulerInterface {
        return match ($type) {
            self::TYPE_TIME_SPENT_BASED => new TimeSpentBasedScheduler($storage, $options),
            default => new LastProcessingBasedScheduler($storage),
        };
    }
}
