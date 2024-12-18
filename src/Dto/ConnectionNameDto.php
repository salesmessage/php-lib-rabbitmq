<?php

namespace Salesmessage\LibRabbitMQ\Dto;

class ConnectionNameDto
{
    private const VHOST_CONNECTION_PREFIX = ':vhost:';

    private string $connectionName  = '';

    private string $configName = '';

    private ?string $vhostName = null;

    private bool $isRabbitVhostConnection = false;

    /**
     * @param string $connectionName
     */
    public function __construct(string $connectionName)
    {
        $this->connectionName = $connectionName;

        $this->isRabbitVhostConnection = str_contains($connectionName, self::VHOST_CONNECTION_PREFIX);

        $configName = $connectionName;
        $vhostName = null;
        if ($this->isRabbitVhostConnection) {
            $vhostData = explode(self::VHOST_CONNECTION_PREFIX, $connectionName);

            $configName = reset($vhostData);
            $vhostName = end($vhostData);
        }

        $this->configName = $configName;
        $this->vhostName = $vhostName;
    }

    /**
     * @param string $vhostName
     * @param string $configName
     * @return string
     */
    public static function getVhostConnectionName(string $vhostName, string $configName = 'rabbitmq_vhosts'): string
    {
        return $configName . self::VHOST_CONNECTION_PREFIX . $vhostName;
    }

    /**
     * @return string
     */
    public function getConnectionName(): string
    {
        return $this->connectionName;
    }

    /**
     * @return string
     */
    public function getConfigName(): string
    {
        return $this->configName;
    }

    /**
     * @return string|null
     */
    public function getVhostName(): ?string
    {
        return $this->vhostName;
    }

    /**
     * @return bool
     */
    public function isRabbitVhostConnection(): bool
    {
        return $this->isRabbitVhostConnection;
    }
}
