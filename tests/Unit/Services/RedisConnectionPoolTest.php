<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services;

use Salesmessage\LibRabbitMQ\Services\RedisConnectionPool;
use Salesmessage\LibRabbitMQ\Tests\Support\RedisBackedTestCase;

class RedisConnectionPoolTest extends RedisBackedTestCase
{
    /**
     * Outside a coroutine there is no concurrency, so the pool passes commands
     * straight through to the wrapped connection. (The coroutine borrow/return
     * path requires a running Swoole scheduler and is exercised in integration.)
     */
    public function testDelegatesCommandsToWrappedConnection(): void
    {
        $pool = new RedisConnectionPool($this->redis);

        $pool->hset('rabbitmq_vhost|pooltest', 'field', '7');

        $this->assertSame('7', $pool->hget('rabbitmq_vhost|pooltest', 'field'));
        $this->assertSame('7', $this->redis->hget('rabbitmq_vhost|pooltest', 'field'));
    }
}
