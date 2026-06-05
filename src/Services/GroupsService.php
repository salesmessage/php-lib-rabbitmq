<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Symfony\Component\Yaml\Yaml;
use Throwable;

class GroupsService
{
    /**
     * @var string
     */
    private string $connectionName = 'rabbitmq_vhosts';

    /**
     * @var array
     */
    private array $configData = [];

    /**
     * @var bool
     */
    private bool $isConfigLoaded = false;


    /**
     * @param string $connectionName
     * @return $this
     */
    public function setConnection(string $connectionName): self
    {
        $this->connectionName = $connectionName;

        $this->configData = $this->loadConfigData();

        $this->isConfigLoaded = true;

        return $this;
    }

    /**
     * @param string $groupName
     * @return array
     */
    public function getGroupConfig(string $groupName): array
    {
        if (false === $this->isConfigLoaded) {
            $this->setConnection($this->connectionName);
        }

        return (array) ($this->configData['groups'][$groupName] ?? []);
    }

    /**
     * @return array
     */
    public function getAllGroupsNames(): array
    {
        if (false === $this->isConfigLoaded) {
            $this->setConnection($this->connectionName);
        }

        if (!isset($this->configData['groups']) || !is_array($this->configData['groups'])) {
            return [];
        }

        return array_filter(array_keys($this->configData['groups']));
    }

    /**
     * @return array
     */
    private function loadConfigData(): array
    {
        $filePath = $this->getConfigFilePath();
        if (!file_exists($filePath)) {
            return [];
        }

        try {
            $configData = (array) Yaml::parseFile($filePath);
        } catch (Throwable $exception) {
            $configData = [];
        }

        return $configData;
    }

    /**
     * @return string
     */
    private function getConfigFilePath(): string
    {
        $fileName = ('rabbitmq_vhosts' === $this->connectionName)
            ? 'rabbit-groups.yml'
            : str_replace('_', '-', $this->connectionName) . '-groups.yml';

        return base_path() . '/' . $fileName;
    }
}

