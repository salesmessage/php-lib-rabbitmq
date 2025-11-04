<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel;

class NullDeduplicationStore implements DeduplicationStore
{
    public function get(string $messageKey): mixed
    {
        return null;
    }

    public function set(string $messageKey, mixed $value, int $ttlSeconds, bool $withOverride = false): bool
    {
        return true;
    }

    public function release(string $messageKey): void {}
}
