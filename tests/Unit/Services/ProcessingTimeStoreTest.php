<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services;

use Salesmessage\LibRabbitMQ\Services\ProcessingTimeStore;
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
