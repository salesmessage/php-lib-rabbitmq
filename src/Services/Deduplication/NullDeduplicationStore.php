<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

class NullDeduplicationStore implements DeduplicationStore
{
    public function add(string $messageKey, int $ttlSeconds): bool
    {
        return true;
    }

    public function release(string $messageKey): void {}
}
