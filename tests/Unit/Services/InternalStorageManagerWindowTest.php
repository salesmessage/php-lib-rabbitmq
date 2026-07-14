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

    public function testNegativeSumIsClampedToZero(): void
    {
        $this->indexVhost('v1', ['g']);

        $this->storage->recordProcessingTime('g', 'v1', -700, 600, 60);

        $this->assertSame('0', $this->windowCost('v1', 'g'));
    }

    public function testProvisionalReconciliationSumsToExactRealTime(): void
    {
        $this->indexVhost('v1', ['g']);

        $this->storage->recordProcessingTime('g', 'v1', 5000, 600, 60);  // provisional
        $this->storage->recordProcessingTime('g', 'v1', -3800, 600, 60); // reconcile 1200 - 5000

        $this->assertSame('1200', $this->windowCost('v1', 'g'));
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
