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

    public function get(string $messageKey): mixed
    {
        $key = $this->getKey($messageKey);
        return $this->connection()->get($key);
    }

    public function set(string $messageKey, mixed $value, int $ttlSeconds, bool $withOverride = false): bool
    {
        if ($ttlSeconds <= 0) {
            throw new \InvalidArgumentException('Invalid TTL seconds. Should be greater than 0.');
        }

        $key = $this->getKey($messageKey);
        $args = [$key, $value, 'EX', $ttlSeconds];
        if (!$withOverride) {
            $args[] = 'NX';
        }

        return (bool) $this->connection()->set(...$args);
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
