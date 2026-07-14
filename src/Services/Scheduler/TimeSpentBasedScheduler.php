<?php

namespace Salesmessage\LibRabbitMQ\Services\Scheduler;

use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;

/**
 * Time-based fair round-robin: the next vhost is the one that consumed the least
 * processing time for the group within a sliding window. Ordering is done on the
 * materialized window_cost:<group> hash field.
 *
 * Anti-stampede for many simultaneous workers:
 *  - vhosts sharing the same (e.g. zero) cost are shuffled so workers do not all
 *    start on the same vhost;
 *  - reserve() adds a provisional cost so a vhost being processed is deprioritized
 *    before any real time is recorded; record() reconciles it exactly.
 *
 * Queue ordering within a vhost stays recency-based (last_processed_at) - fairness
 * is applied at the vhost level only.
 */
class TimeSpentBasedScheduler implements VhostSchedulerInterface
{
    private int $window;

    private int $bucket;

    /**
     * Reservation estimate converted from config seconds to integer milliseconds.
     * All time accounting is done in integer milliseconds so provisional charges
     * reconcile to exact zero (no float residue) and Redis can use HINCRBY.
     */
    private int $reservationEstimateMs;

    /**
     * Provisional costs added by reserve() and not yet reconciled by a record(),
     * as [group => [vhost => milliseconds]]. A worker holds one selection at a
     * time, so any provisional for another vhost belongs to an abandoned
     * selection (empty queue, retry) and is refunded on the next reserve().
     */
    private array $pendingProvisional = [];

    /**
     * @param InternalStorageManager $storage
     * @param array $options
     */
    public function __construct(private InternalStorageManager $storage, array $options = [])
    {
        $this->window = max(1, (int) ($options['window'] ?? 600));
        $this->bucket = max(1, (int) ($options['bucket'] ?? 60));
        $this->reservationEstimateMs = max(0, (int) round(
            (float) ($options['reservation_estimate'] ?? 5.0) * 1000
        ));
    }

    /**
     * @inheritDoc
     */
    public function getOrderedVhosts(string $group): array
    {
        $rows = $this->storage->getVhostsWithWeights($this->storage->getWindowCostKeyName($group));

        return $this->shuffleEqualWeights($rows);
    }

    /**
     * @inheritDoc
     */
    public function getOrderedQueues(string $group, string $vhost): array
    {
        return $this->storage->getVhostQueues($vhost, $this->storage->getLastProcessedAtKeyName($group), false);
    }

    /**
     * @inheritDoc
     */
    public function reserve(string $group, string $vhost, string $queue): void
    {
        $this->storage->touchLastProcessedAt($group, $vhost, $queue);

        if ($this->reservationEstimateMs <= 0) {
            return;
        }

        $this->refundPendingProvisionals();

        $this->storage->recordProcessingTime(
            $group,
            $vhost,
            $this->reservationEstimateMs,
            $this->window,
            $this->bucket
        );
        $this->pendingProvisional[$group][$vhost] = $this->reservationEstimateMs;
    }

    /**
     * @inheritDoc
     */
    public function record(string $group, string $vhost, string $queue, int $milliseconds): void
    {
        $this->storage->touchLastProcessedAt($group, $vhost, $queue);

        $pending = (int) ($this->pendingProvisional[$group][$vhost] ?? 0);
        unset($this->pendingProvisional[$group][$vhost]);

        $this->storage->recordProcessingTime(
            $group,
            $vhost,
            $milliseconds - $pending,
            $this->window,
            $this->bucket
        );
    }

    /**
     * Refund provisional costs of selections that never produced a record()
     * (empty queue, connection retry), so abandoned vhosts are not left with
     * phantom cost until bucket expiry.
     *
     * @return void
     */
    private function refundPendingProvisionals(): void
    {
        foreach ($this->pendingProvisional as $group => $vhosts) {
            foreach ($vhosts as $vhost => $milliseconds) {
                $this->storage->recordProcessingTime(
                    $group,
                    (string) $vhost,
                    -$milliseconds,
                    $this->window,
                    $this->bucket
                );
            }
        }

        $this->pendingProvisional = [];
    }

    /**
     * Preserve the ascending-by-weight order but shuffle vhosts that share the
     * same weight, so simultaneous workers spread across equal-cost vhosts.
     *
     * @param array $rows list of ['name' => string, 'weight' => float]
     * @return array
     */
    private function shuffleEqualWeights(array $rows): array
    {
        $ordered = [];
        $run = [];
        $runWeight = null;

        foreach ($rows as $row) {
            if (null !== $runWeight && $row['weight'] !== $runWeight) {
                shuffle($run);
                array_push($ordered, ...$run);
                $run = [];
            }

            $runWeight = $row['weight'];
            $run[] = $row['name'];
        }

        if (!empty($run)) {
            shuffle($run);
            array_push($ordered, ...$run);
        }

        return $ordered;
    }
}
