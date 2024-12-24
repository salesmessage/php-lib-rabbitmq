<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Symfony\Component\Yaml\Yaml;

class GroupsService
{
    /**
     * @var array
     */
    private array $configData = [];

    public function __construct()
    {
        try {
            $configData = (array) Yaml::parseFile(base_path() . '/rabbit-groups.yml');
        } catch (Throwable $exception) {
            $configData = [];
        }

        $this->configData = $configData;
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
}

