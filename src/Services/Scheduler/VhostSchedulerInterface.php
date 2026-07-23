<?php

namespace Salesmessage\LibRabbitMQ\Services\Scheduler;

interface VhostSchedulerInterface
{
    /**
     * Ordered list of vhost names to consume next for the given group.
     * The first element is the vhost that should be processed next.
     *
     * @param string $group
     * @return array
     */
    public function getOrderedVhosts(string $group): array;

    /**
     * Ordered list of queue names within a vhost to consume next for the given group.
     *
     * @param string $group
     * @param string $vhost
     * @return array
     */
    public function getOrderedQueues(string $group, string $vhost): array;

    /**
     * Mark a vhost/queue as being taken for processing right now.
     * Used to deprioritize it against other workers before any work is recorded.
     *
     * @param string $group
     * @param string $vhost
     * @param string $queue
     * @return void
     */
    public function reserve(string $group, string $vhost, string $queue): void;

    /**
     * Record how much processing time was spent on a vhost/queue.
     *
     * @param string $group
     * @param string $vhost
     * @param string $queue
     * @param int $milliseconds
     * @return void
     */
    public function record(string $group, string $vhost, string $queue, int $milliseconds): void;

    /**
     * Flush the processing time accrued so far by an in-flight job, so a long
     * running job is reflected in the fairness ordering before it completes
     * instead of only at record() time. Called periodically while a job runs.
     *
     * @param string $group
     * @param string $vhost
     * @param int $elapsedMs processing time elapsed so far for the current job
     * @return void
     */
    public function accrue(string $group, string $vhost, int $elapsedMs): void;
}
