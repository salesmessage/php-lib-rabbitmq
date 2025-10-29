<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

interface DeduplicationStore
{
    public function add(string $messageKey, int $ttlSeconds): bool;

    public function release(string $messageKey): void;
}
