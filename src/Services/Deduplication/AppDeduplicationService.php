<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

class AppDeduplicationService
{
    /**
     * @return bool
     */
    public static function isEnabled(): bool
    {
        return (bool) static::getConfig('enabled', true);
    }

    /**
     * @param string $key
     * @param mixed|null $default
     * @return mixed
     */
    protected static function getConfig(
        string $key,
        mixed $default = null
    ): mixed
    {
        $value = config("queue.drivers.rabbitmq_vhosts.deduplication.application.$key");

        return $value !== null ? $value : $default;
    }
}

