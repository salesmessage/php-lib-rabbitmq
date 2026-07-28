<?php

namespace Salesmessage\LibRabbitMQ\Services\Scheduler;

use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;

class VhostSchedulerFactory
{
    public const STRATEGY_LAST_PROCESSED = 'last_processed';

    public const STRATEGY_PROCESSING_TIME = 'processing_time';

    /**
     * Build a scheduler from a strategy string and its options.
     * Unknown strategies fall back to the recency-based scheduler.
     *
     * @param InternalStorageManager $storage
     * @param string $strategy
     * @param array $options options for processing_time: window, bucket, reservation_estimate
     * @return VhostSchedulerInterface
     */
    public static function make(
        InternalStorageManager $storage,
        string $strategy,
        array $options = []
    ): VhostSchedulerInterface {
        return match ($strategy) {
            self::STRATEGY_PROCESSING_TIME => new ProcessingTimeSchedulerStrategy($storage, $options),
            default => new LastProcessedSchedulerStrategy($storage),
        };
    }
}
