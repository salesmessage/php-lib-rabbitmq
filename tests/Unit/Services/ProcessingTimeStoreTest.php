<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services;

use Salesmessage\LibRabbitMQ\Services\ProcessingTimeStore;
use Salesmessage\LibRabbitMQ\Tests\Support\FakeRedisConnection;
use Salesmessage\LibRabbitMQ\Tests\Support\RedisBackedTestCase;

/**
 * The store is entity-agnostic: it operates on whatever buckets/owner/index keys
 * it is handed, which is exactly how it serves both vhosts and queues.
 */
class ProcessingTimeStoreTest extends RedisBackedTestCase
{
    private ProcessingTimeStore $store;

    protected function setUp(): void
    {
        parent::setUp();

        $this->store = new ProcessingTimeStore($this->redis);
    }

    public function testRecordAccumulatesAndMaterializesCostOnOwner(): void
    {
        $this->redis->hset('rabbitmq_vhost|owner', 'name', 'owner');

        $this->store->record('rabbitmq_proc_buckets|owner', 'rabbitmq_vhost|owner', 'window_cost:g', 1500, 600, 60);
        $this->store->record('rabbitmq_proc_buckets|owner', 'rabbitmq_vhost|owner', 'window_cost:g', 1500, 600, 60);

        $this->assertSame('3000', $this->redis->hget('rabbitmq_vhost|owner', 'window_cost:g'));
    }

    public function testNegativeSumClearsTheMaterializedField(): void
    {
        $this->redis->hset('rabbitmq_vhost|owner', 'name', 'owner');

        $this->store->record('rabbitmq_proc_buckets|owner', 'rabbitmq_vhost|owner', 'window_cost:g', -700, 600, 60);

        $this->assertNull($this->redis->hget('rabbitmq_vhost|owner', 'window_cost:g'));
    }

    public function testDoesNotResurrectAMissingOwner(): void
    {
        // owner hash does not exist: a positive cost must not conjure it
        $this->store->record('rabbitmq_proc_buckets|ghost', 'rabbitmq_vhost|ghost', 'window_cost:g', 1500, 600, 60);

        $this->assertSame(0, (int) $this->redis->exists('rabbitmq_vhost|ghost'));
    }

    public function testRefreshManyRefreshesEveryEntryInOnePipeline(): void
    {
        $this->redis->hset('rabbitmq_vhost|a', 'name', 'a');
        $this->redis->hset('rabbitmq_vhost|b', 'name', 'b');

        $currentBucket = intdiv(time(), 60);
        $expiredBucket = $currentBucket - 100;

        $this->redis->hincrby('rabbitmq_proc_buckets|a', (string) $currentBucket, 2000);
        $this->redis->hincrby('rabbitmq_proc_buckets|a', (string) $expiredBucket, 5000);
        $this->redis->hincrby('rabbitmq_proc_buckets|b', (string) $expiredBucket, 7000);

        $this->store->refreshMany([
            ['rabbitmq_proc_buckets|a', 'rabbitmq_vhost|a', 'window_cost:g'],
            ['rabbitmq_proc_buckets|b', 'rabbitmq_vhost|b', 'window_cost:g'],
        ], 600, 60);

        $this->assertSame('2000', $this->redis->hget('rabbitmq_vhost|a', 'window_cost:g'));
        $this->assertNull($this->redis->hget('rabbitmq_vhost|b', 'window_cost:g'));
        $this->assertSame(0, (int) $this->redis->hexists('rabbitmq_proc_buckets|a', (string) $expiredBucket));
    }

    public function testRefreshManyFallsBackPerEntryWhenScriptCacheIsFlushed(): void
    {
        if (static::useRealRedis()) {
            $this->markTestSkipped('Simulates a flushed server script cache; fake-only.');
        }

        // SCRIPT LOAD that does not persist the script, as if the server cache
        // was flushed (restart) between the load and the pipelined EVALSHA
        $redis = new class extends FakeRedisConnection {
            public function script($subcommand, ...$arguments)
            {
                return sha1((string) ($arguments[0] ?? ''));
            }
        };
        $store = new ProcessingTimeStore($redis);

        $redis->hset('rabbitmq_vhost|a', 'name', 'a');
        $redis->hincrby('rabbitmq_proc_buckets|a', (string) intdiv(time(), 60), 2000);

        $store->refreshMany([
            ['rabbitmq_proc_buckets|a', 'rabbitmq_vhost|a', 'window_cost:g'],
        ], 600, 60);

        $this->assertSame('2000', $redis->hget('rabbitmq_vhost|a', 'window_cost:g'));
    }

    public function testRefreshManyWithNoEntriesIsANoop(): void
    {
        $this->expectNotToPerformAssertions();

        $this->store->refreshMany([], 600, 60);
    }

    public function testOrderedByWeightSortsAscendingAndStripsPrefix(): void
    {
        $this->redis->sadd('rabbitmq_vhosts_index', ['rabbitmq_vhost|a', 'rabbitmq_vhost|b']);
        $this->redis->hset('rabbitmq_vhost|a', 'window_cost:g', '50');
        $this->redis->hset('rabbitmq_vhost|b', 'window_cost:g', '10');

        $rows = $this->store->orderedByWeight('rabbitmq_vhosts_index', 'rabbitmq_vhost|', 'window_cost:g');

        $this->assertSame([
            ['name' => 'b', 'weight' => 10.0],
            ['name' => 'a', 'weight' => 50.0],
        ], $rows);
    }
}
