<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

class AppDeduplicationService
{
    public static function isEnabled(): bool
    {
        return (bool) static::getConfig('enabled', true);
    }

    protected static function getConfig(string $key, mixed $default = null): mixed
    {
        $value = config("queue.connections.rabbitmq_vhosts.deduplication.application.$key");

        return $value !== null ? $value : $default;
    }
}
