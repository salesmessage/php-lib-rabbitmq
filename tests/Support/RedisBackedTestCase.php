<?php

namespace Salesmessage\LibRabbitMQ\Tests\Support;

use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use Predis\Client as PredisClient;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Tests\TestCase;

/**
 * Base for tests running against Redis-backed storage.
 *
 * By default an in-memory FakeRedisConnection is used, so the suite needs no
 * external services. Set USE_REAL_REDIS=1 to run the very same tests against a
 * live Redis (TEST_REDIS_HOST/TEST_REDIS_PORT, default 127.0.0.1:6379): every
 * key is transparently prefixed with sm_test_rabbitmq_lib_ via the Predis
 * prefix option, and all keys with that prefix are purged before and after
 * each test.
 */
abstract class RedisBackedTestCase extends TestCase
{
    public const TEST_KEY_PREFIX = 'sm_test_rabbitmq_lib_';

    protected PredisConnection $redis;

    protected InternalStorageManager $storage;

    private ?PredisClient $cleanupClient = null;

    protected static function useRealRedis(): bool
    {
        return filter_var((string) getenv('USE_REAL_REDIS'), FILTER_VALIDATE_BOOL);
    }

    protected function setUp(): void
    {
        parent::setUp();

        if (static::useRealRedis()) {
            $parameters = [
                'host' => getenv('TEST_REDIS_HOST') ?: '127.0.0.1',
                'port' => (int) (getenv('TEST_REDIS_PORT') ?: 6379),
            ];

            $this->redis = new PredisConnection(
                new PredisClient($parameters, ['prefix' => self::TEST_KEY_PREFIX])
            );
            $this->cleanupClient = new PredisClient($parameters);

            $this->flushTestKeys();
        } else {
            $this->redis = new FakeRedisConnection();
        }

        $connection = $this->redis;
        Redis::swap(new class($connection) {
            public function __construct(private PredisConnection $connection)
            {
            }

            public function connection($name = null): PredisConnection
            {
                return $this->connection;
            }
        });

        $this->storage = new InternalStorageManager();
    }

    protected function tearDown(): void
    {
        $this->flushTestKeys();
        $this->cleanupClient = null;

        parent::tearDown();
    }

    private function flushTestKeys(): void
    {
        if (null === $this->cleanupClient) {
            return;
        }

        $cursor = 0;
        do {
            [$cursor, $keys] = $this->cleanupClient->scan($cursor, [
                'match' => self::TEST_KEY_PREFIX . '*',
                'count' => 1000,
            ]);

            if (!empty($keys)) {
                $this->cleanupClient->del($keys);
            }
        } while (0 !== (int) $cursor);
    }

    protected function indexVhost(string $name, array $groups): void
    {
        $this->storage->indexVhost(new VhostApiDto([
            'name' => $name,
            'messages' => 1,
            'messages_ready' => 1,
        ]), $groups);
    }

    protected function indexQueue(string $vhost, string $queue, array $groups): void
    {
        $this->storage->indexQueue(new QueueApiDto([
            'name' => $queue,
            'vhost' => $vhost,
            'messages' => 1,
            'messages_ready' => 1,
        ]), $groups);
    }

    protected function windowCost(string $vhost, string $group): ?string
    {
        return $this->redis->hget('rabbitmq_vhost|' . $vhost, 'window_cost:' . $group);
    }

    protected function queueWindowCost(string $vhost, string $queue, string $group): ?string
    {
        return $this->redis->hget('rabbitmq_queue|' . $vhost . '|' . $queue, 'window_cost:' . $group);
    }

    protected function bucketsKey(string $group, string $vhost): string
    {
        return sprintf('rabbitmq_proc_buckets|%s|%s', $group, $vhost);
    }
}
