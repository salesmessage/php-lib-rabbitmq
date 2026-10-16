<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services;

use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Tests\Support\FakeRedisConnection;
use Salesmessage\LibRabbitMQ\Tests\TestCase;

class InternalStorageManagerConnectionTest extends TestCase
{
    private object $redisManager;

    protected function setUp(): void
    {
        parent::setUp();

        $this->redisManager = new class(new FakeRedisConnection)
        {
            /** @var array<int, string|null> */
            public array $connected = [];

            /** @var array<int, string|null> */
            public array $resolved = [];

            public function __construct(private PredisConnection $connection) {}

            public function connection($name = null): PredisConnection
            {
                $this->connected[] = $name;

                return $this->connection;
            }

            public function resolve($name = null): PredisConnection
            {
                $this->resolved[] = $name;

                return $this->connection;
            }
        };

        Redis::swap($this->redisManager);
    }

    public function test_uses_persistent_connection_when_storage_connection_is_not_configured(): void
    {
        $storage = new InternalStorageManager;
        $storage->usePooledConnection();

        $this->assertSame(['persistent'], $this->redisManager->connected);
        $this->assertSame(['persistent'], $this->redisManager->resolved);
    }

    public function test_uses_configured_storage_connection(): void
    {
        config()->set('queue.drivers.rabbitmq_vhosts.storage.connection', 'vhosts_storage');

        $storage = new InternalStorageManager;
        $storage->usePooledConnection();

        $this->assertSame(['vhosts_storage'], $this->redisManager->connected);
        $this->assertSame(['vhosts_storage'], $this->redisManager->resolved);
    }
}
