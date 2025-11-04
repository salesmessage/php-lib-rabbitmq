<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel;

interface DeduplicationStore
{
    public function get(string $messageKey): mixed;

    public function set(string $messageKey, mixed $value, int $ttlSeconds, bool $withOverride = false): bool;

    public function release(string $messageKey): void;
}
