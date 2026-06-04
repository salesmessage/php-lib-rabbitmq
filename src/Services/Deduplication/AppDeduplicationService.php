<?php

namespace Salesmessage\LibRabbitMQ\Services\Deduplication;

class AppDeduplicationService
{
    /**
     * @param string $connectionName
     * @return bool
     */
    public static function isEnabled(string $connectionName = 'rabbitmq_vhosts'): bool
    {
        return (bool) static::getConfig('enabled', true, $connectionName);
    }

    /**
     * @param string $key
     * @param mixed|null $default
     * @param string $connectionName
     * @return mixed
     */
    protected static function getConfig(
        string $key,
        mixed $default = null,
        string $connectionName = 'rabbitmq_vhosts'
    ): mixed
    {
        $value = config("queue.connections.$connectionName.deduplication.application.$key");

        return $value !== null ? $value : $default;
    }
}

