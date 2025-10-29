<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

interface DeduplicationStore
{
    public function get(string $messageKey): mixed;

    public function add(string $messageKey, mixed $value, int $ttlSeconds): bool;

    public function release(string $messageKey): void;
}
