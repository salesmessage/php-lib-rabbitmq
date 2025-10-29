<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\Redis;

class RedisDeduplicationStore implements DeduplicationStore
{
    public function __construct(
        protected ?string $connectionName = null,
        protected string $keyPrefix = 'mq_dedup',
    ) {}

    public function add(string $messageKey, int $ttlSeconds): bool
    {
        if ($ttlSeconds <= 0 || $ttlSeconds > 7 * 24 * 60 * 60) {
            throw new \InvalidArgumentException('Invalid TTL seconds');
        }

        $key = $this->getKey($messageKey);

        return (bool) $this->connection()->set($key, 1, 'EX', $ttlSeconds, 'NX');
    }

    public function release(string $messageKey): void
    {
        $key = $this->getKey($messageKey);
        $this->connection()->del($key);
    }

    protected function connection(): Connection
    {
        return $this->connectionName ? Redis::connection($this->connectionName) : Redis::connection();
    }

    protected function getKey(string $messageKey): string
    {
        return $this->keyPrefix . ':' . $messageKey;
    }
}
