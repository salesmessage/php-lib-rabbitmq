<?php

namespace Salesmessage\LibRabbitMQ\Services\Scheduler;

use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;

/**
 * Recency-based round-robin: the next vhost/queue is the one whose last
 * processing happened longest ago (ordered by the last_processed_at timestamp).
 */
class LastProcessedSchedulerStrategy implements VhostSchedulerInterface
{
    /**
     * @param InternalStorageManager $storage
     */
    public function __construct(private InternalStorageManager $storage)
    {
    }

    /**
     * @inheritDoc
     */
    public function getOrderedVhosts(string $group): array
    {
        return $this->storage->getVhosts($this->storage->getLastProcessedAtKeyName($group), false);
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
    }

    /**
     * @inheritDoc
     */
    public function record(string $group, string $vhost, string $queue, int $milliseconds): void
    {
        $this->storage->touchLastProcessedAt($group, $vhost, $queue);
    }
}
