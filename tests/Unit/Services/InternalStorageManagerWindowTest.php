<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services;

use Salesmessage\LibRabbitMQ\Tests\Support\RedisBackedTestCase;

class InternalStorageManagerWindowTest extends RedisBackedTestCase
{
    public function testRecordAccumulatesIntoCurrentBucketAndMaterializesCost(): void
    {
        $this->indexVhost('v1', ['g']);

        $this->storage->recordProcessingTime('g', 'v1', 1500, 600, 60);
        $this->storage->recordProcessingTime('g', 'v1', 1500, 600, 60);

        $this->assertSame('3000', $this->windowCost('v1', 'g'));

        $buckets = $this->redis->hgetall($this->bucketsKey('g', 'v1'));
        $this->assertCount(1, $buckets);
        $this->assertSame('3000', reset($buckets));

        $ttl = $this->redis->ttl($this->bucketsKey('g', 'v1'));
        $this->assertGreaterThan(0, $ttl);
        $this->assertLessThanOrEqual(660, $ttl);
    }

    public function testExpiredBucketsAreTrimmedAndExcludedFromCost(): void
    {
        $this->indexVhost('v1', ['g']);

        $currentBucket = intdiv(time(), 60);
        $expiredBucket = $currentBucket - 100;

        $bucketsKey = $this->bucketsKey('g', 'v1');
        $this->redis->hincrby($bucketsKey, (string) $currentBucket, 2000);
        $this->redis->hincrby($bucketsKey, (string) $expiredBucket, 5000);

        $cost = $this->storage->refreshWindowCost('g', 'v1', 600, 60);

        $this->assertSame(2000, $cost);
        $this->assertSame('2000', $this->windowCost('v1', 'g'));
        $this->assertSame(0, (int) $this->redis->hexists($bucketsKey, (string) $expiredBucket));
    }

    public function testNegativeSumClearsCostInsteadOfGoingNegative(): void
    {
        $this->indexVhost('v1', ['g']);

        $this->storage->recordProcessingTime('g', 'v1', -700, 600, 60);

        // no live cost -> field is dropped; SORT treats a missing field as 0
        $this->assertNull($this->windowCost('v1', 'g'));
    }

    public function testProvisionalReconciliationSumsToExactRealTime(): void
    {
        $this->indexVhost('v1', ['g']);

        $this->storage->recordProcessingTime('g', 'v1', 5000, 600, 60);  // provisional
        $this->storage->recordProcessingTime('g', 'v1', -3800, 600, 60); // reconcile 1200 - 5000

        $this->assertSame('1200', $this->windowCost('v1', 'g'));
    }

    public function testNegativeBucketDoesNotDriveCostBelowZeroWhenSumIsPositive(): void
    {
        $this->indexVhost('v1', ['g']);

        $currentBucket = intdiv(time(), 60);
        $bucketsKey = $this->bucketsKey('g', 'v1');

        // a positive charge in one bucket and a negative reconciliation in the
        // next: the individual bucket may be negative, the materialized cost is
        // the (positive) sum across live buckets
        $this->redis->hincrby($bucketsKey, (string) ($currentBucket - 1), 5000);
        $this->redis->hincrby($bucketsKey, (string) $currentBucket, -1800);

        $cost = $this->storage->refreshWindowCost('g', 'v1', 600, 60);

        $this->assertSame(3200, $cost);
        $this->assertSame('3200', $this->windowCost('v1', 'g'));
    }

    public function testDoesNotResurrectMissingVhostKeyWhenMaterializingCost(): void
    {
        // vhost is deliberately NOT indexed, so its storage hash does not exist
        $storageKey = 'rabbitmq_vhost|ghost';

        $this->storage->recordProcessingTime('g', 'ghost', 1500, 600, 60);

        // a positive cost must not conjure the vhost key into existence
        $this->assertSame(0, (int) $this->redis->exists($storageKey));
        $this->assertNull($this->windowCost('ghost', 'g'));

        // the buckets themselves are independent and still recorded
        $buckets = $this->redis->hgetall($this->bucketsKey('g', 'ghost'));
        $this->assertSame('1500', reset($buckets));
    }

    public function testBatchedRefreshWindowCostsCoversEveryGroup(): void
    {
        $this->indexVhost('v1', ['g1', 'g2']);

        $currentBucket = intdiv(time(), 60);
        $expiredBucket = $currentBucket - 100;

        $this->redis->hincrby($this->bucketsKey('g1', 'v1'), (string) $currentBucket, 2000);
        $this->redis->hincrby($this->bucketsKey('g1', 'v1'), (string) $expiredBucket, 5000);
        $this->redis->hincrby($this->bucketsKey('g2', 'v1'), (string) $expiredBucket, 7000);

        $this->storage->refreshWindowCosts(['g1', 'g2'], 'v1', 600, 60);

        $this->assertSame('2000', $this->windowCost('v1', 'g1'));
        $this->assertNull($this->windowCost('v1', 'g2'));
    }

    public function testBatchedRefreshQueueWindowCostsCoversEveryGroupAndQueue(): void
    {
        $this->indexVhost('v1', ['g1', 'g2']);
        $this->indexQueue('v1', 'q1', ['g1', 'g2']);
        $this->indexQueue('v1', 'q2', ['g1', 'g2']);

        $this->storage->recordQueueProcessingTime('g1', 'v1', 'q1', 3000, 600, 60);
        $this->storage->recordQueueProcessingTime('g2', 'v1', 'q2', 4000, 600, 60);

        $expiredBucket = intdiv(time(), 60) - 100;
        $this->redis->hincrby(
            sprintf('rabbitmq_proc_buckets|%s|%s|%s', 'g2', 'v1', 'q2'),
            (string) $expiredBucket,
            6000
        );

        $this->storage->refreshQueueWindowCosts(['g1', 'g2'], 'v1', ['q1', 'q2'], 600, 60);

        $this->assertSame('3000', $this->queueWindowCost('v1', 'q1', 'g1'));
        $this->assertSame('4000', $this->queueWindowCost('v1', 'q2', 'g2'));
        $this->assertNull($this->queueWindowCost('v1', 'q2', 'g1'));
        $this->assertNull($this->queueWindowCost('v1', 'q1', 'g2'));
    }

    public function testGetVhostsWithWeightsReturnsAscendingCostOrder(): void
    {
        $this->indexVhost('v1', ['g']);
        $this->indexVhost('v2', ['g']);
        $this->indexVhost('v3', ['g']);

        $this->storage->recordProcessingTime('g', 'v1', 100, 600, 60);
        $this->storage->recordProcessingTime('g', 'v2', 50, 600, 60);

        $rows = $this->storage->getVhostsWithWeights($this->storage->getWindowCostKeyName('g'));

        $this->assertSame(
            [['name' => 'v3', 'weight' => 0.0], ['name' => 'v2', 'weight' => 50.0], ['name' => 'v1', 'weight' => 100.0]],
            $rows
        );
    }

    public function testTouchLastProcessedAtUpdatesVhostAndQueue(): void
    {
        $this->indexVhost('v1', ['g']);
        $this->indexQueue('v1', 'q1', ['g']);

        $this->storage->touchLastProcessedAt('g', 'v1', 'q1');

        $now = time();
        $this->assertEqualsWithDelta($now, (int) $this->redis->hget('rabbitmq_vhost|v1', 'last_processed_at:g'), 2);
        $this->assertEqualsWithDelta($now, (int) $this->redis->hget('rabbitmq_queue|v1|q1', 'last_processed_at:g'), 2);
    }
}
