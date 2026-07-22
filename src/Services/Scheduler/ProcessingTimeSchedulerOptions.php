<?php

namespace Salesmessage\LibRabbitMQ\Services\Scheduler;

final class ProcessingTimeSchedulerOptions
{
    private const DEFAULT_WINDOW = 300;

    private const DEFAULT_BUCKET = 30;

    private const DEFAULT_RESERVATION_ESTIMATE = 5;

    private const MIN_WINDOW = 60;

    private const MIN_RESERVATION_MS = 1000;

    /**
     * @param int $window
     * @param int $bucket
     * @param int $reservationEstimateMs
     */
    private function __construct(
        private int $window,
        private int $bucket,
        private int $reservationEstimateMs
    ) {
    }

    /**
     * @param array $options raw config: window (s), bucket (s), reservation_estimate (s)
     * @return self
     */
    public static function fromConfig(array $options): self
    {
        $window = max(self::MIN_WINDOW, (int) ($options['window'] ?? self::DEFAULT_WINDOW));
        $bucket = min($window, max(1, (int) ($options['bucket'] ?? self::DEFAULT_BUCKET)));

        $reservationSeconds = (float) ($options['reservation_estimate'] ?? self::DEFAULT_RESERVATION_ESTIMATE);

        // 0 (or negative) disables the provisional charge entirely; positive
        // values convert to integer milliseconds with a 1s floor - integer math
        // keeps provisional reconciliation exact and HINCRBY-compatible
        $reservationEstimateMs = ($reservationSeconds > 0)
            ? max(self::MIN_RESERVATION_MS, (int) round($reservationSeconds * 1000))
            : 0;

        return new self($window, $bucket, $reservationEstimateMs);
    }

    /**
     * @return int
     */
    public function getWindow(): int
    {
        return $this->window;
    }

    /**
     * @return int
     */
    public function getBucket(): int
    {
        return $this->bucket;
    }

    /**
     * @return int
     */
    public function getReservationEstimateMs(): int
    {
        return $this->reservationEstimateMs;
    }
}
