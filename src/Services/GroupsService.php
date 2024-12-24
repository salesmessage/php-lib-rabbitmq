<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Symfony\Component\Yaml\Yaml;
use Throwable;

class GroupsService
{
    /**
     * @var array
     */
    private array $configData = [];

    public function __construct()
    {
        $this->configData = $this->loadConfigData();
    }

    /**
     * @param string $groupName
     * @return array
     */
    public function getGroupConfig(string $groupName): array
    {
        return (array) ($this->configData['groups'][$groupName] ?? []);
    }

    /**
     * @return array
     */
    public function getAllGroupsNames(): array
    {
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
        $filePath = base_path() . '/rabbit-groups.yml';
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
}

