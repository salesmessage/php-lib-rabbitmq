<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

class NullDeduplicationStore implements DeduplicationStore
{
    public function get(string $messageKey): mixed
    {
        return null;
    }

    public function add(string $messageKey, mixed $value, int $ttlSeconds): bool
    {
        return true;
    }

    public function release(string $messageKey): void {}
}
