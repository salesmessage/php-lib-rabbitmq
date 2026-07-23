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
 * The same time-based fairness is applied at the queue level: every job's
 * processing time is accounted against both its vhost and its queue, and queues
 * within a vhost are ordered by their own window cost - so a long-running job on
 * one queue does not starve the other queues in the same vhost.
 */
class ProcessingTimeSchedulerStrategy implements VhostSchedulerInterface
{
    private ProcessingTimeSchedulerOptions $options;

    /**
     * Provisional charges added by reserve() and not yet reconciled by a record(),
     * as [group => [vhost => ['queue' => string, 'ms' => int]]]. A worker holds
     * one selection at a time, so any entry for another vhost belongs to an
     * abandoned selection (empty queue, retry) and is refunded on the next
     * reserve(). The queue is stored so both levels can be reconciled/refunded.
     */
    private array $pendingProvisional = [];

    /**
     * @param InternalStorageManager $storage
     * @param array $options
     */
    public function __construct(private InternalStorageManager $storage, array $options = [])
    {
        $this->options = ProcessingTimeSchedulerOptions::fromConfig($options);
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
        $rows = $this->storage->getVhostQueuesWithWeights($vhost, $this->storage->getWindowCostKeyName($group));

        return $this->shuffleEqualWeights($rows);
    }

    /**
     * @inheritDoc
     */
    public function reserve(string $group, string $vhost, string $queue): void
    {
        $this->storage->touchLastProcessedAt($group, $vhost, $queue);

        $reservationEstimateMs = $this->options->getReservationEstimateMs();
        if ($reservationEstimateMs <= 0) {
            return;
        }

        $this->refundPendingProvisionals();

        $this->chargeProcessingTime($group, $vhost, $queue, $reservationEstimateMs);
        $this->pendingProvisional[$group][$vhost] = ['queue' => $queue, 'ms' => $reservationEstimateMs];
    }

    /**
     * @inheritDoc
     */
    public function record(string $group, string $vhost, string $queue, int $milliseconds): void
    {
        $this->storage->touchLastProcessedAt($group, $vhost, $queue);

        $pending = (int) ($this->pendingProvisional[$group][$vhost]['ms'] ?? 0);
        unset($this->pendingProvisional[$group][$vhost]);

        $this->chargeProcessingTime($group, $vhost, $queue, $milliseconds - $pending);
    }

    /**
     * @inheritDoc
     *
     * Grow the in-flight selection's charge toward the processing time elapsed
     * so far, so a long-running job is reflected in the ordering before it
     * finishes. Only grows upward - record() reconciles the total to the exact
     * real time at the end (which may be a downward correction).
     */
    public function accrue(string $group, string $vhost, int $elapsedMs): void
    {
        if (!isset($this->pendingProvisional[$group][$vhost])) {
            return;
        }

        $charged = (int) $this->pendingProvisional[$group][$vhost]['ms'];
        if ($elapsedMs <= $charged) {
            return;
        }

        $queue = (string) $this->pendingProvisional[$group][$vhost]['queue'];

        // update the tracked amount before the (yielding) Redis writes so a
        // record() interleaving in another coroutine reconciles against it
        $this->pendingProvisional[$group][$vhost]['ms'] = $elapsedMs;

        $this->chargeProcessingTime($group, $vhost, $queue, $elapsedMs - $charged);
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
            foreach ($vhosts as $vhost => $pending) {
                $this->chargeProcessingTime(
                    $group,
                    (string) $vhost,
                    (string) $pending['queue'],
                    -(int) $pending['ms']
                );
            }
        }

        $this->pendingProvisional = [];
    }

    /**
     * Account processing time against both the vhost and its specific queue so
     * ordering is fair at both levels. The amount may be negative (a reconcile
     * or a refund).
     *
     * @param string $group
     * @param string $vhost
     * @param string $queue
     * @param int $milliseconds
     * @return void
     */
    private function chargeProcessingTime(string $group, string $vhost, string $queue, int $milliseconds): void
    {
        $window = $this->options->getWindow();
        $bucket = $this->options->getBucket();

        $this->storage->recordProcessingTime($group, $vhost, $milliseconds, $window, $bucket);
        $this->storage->recordQueueProcessingTime($group, $vhost, $queue, $milliseconds, $window, $bucket);
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
