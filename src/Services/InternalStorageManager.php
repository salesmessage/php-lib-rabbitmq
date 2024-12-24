<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;

/**
 * https://github.com/phpredis/phpredis
 * https://redis.io/docs/latest/commands/sort/
 */
class InternalStorageManager
{
    private const INDEX_KEY_VHOSTS = 'rabbitmq_vhosts_index';

    private const INDEX_KEY_QUEUES = 'rabbitmq_queues_index';

    private PredisConnection $redis;

    public function __construct() {
        /** @var PredisConnection $redis */
        $redis = Redis::connection('persistent');
        $this->redis = $redis;
    }

    /**
     * @param string $by
     * @param bool $alpha
     * @return array
     */
    public function getVhosts(string $by = 'name', bool $alpha = true): array
    {
        $vhosts = $this->redis->sort(self::INDEX_KEY_VHOSTS, [
            'by' => '*->' . $by,
            'alpha' => $alpha,
            'sort' => 'asc',
        ]);

        return array_map(fn($value): string => str_replace_first(
            $this->getVhostStorageKeyPrefix(),
            '',
            $value
        ), $vhosts);
    }

    /**
     * @param string $vhostName
     * @param string $by
     * @param bool $alpha
     * @return array
     */
    public function getVhostQueues(string $vhostName, string $by = 'name', bool $alpha = true): array
    {
        $indexKey = $this->getQueueIndexKey($vhostName);

        $queues = $this->redis->sort($indexKey, [
            'by' => '*->' . $by,
            'alpha' => $alpha,
            'sort' => 'asc',
        ]);

        return array_map(fn($value): string => str_replace_first(
            $this->getQueueStorageKeyPrefix($vhostName),
            '',
            $value
        ), $queues);
    }

    /**
     * @param VhostApiDto $vhostDto
     * @param array $groups
     * @return bool
     */
    public function indexVhost(VhostApiDto $vhostDto, array $groups = []): bool
    {
        if ($vhostDto->getMessagesReady() > 0) {
            return $this->addVhost($vhostDto, $groups);
        }

        return $this->removeVhost($vhostDto);
    }

    /**
     * @param VhostApiDto $vhostDto
     * @param array $groups
     * @return bool
     */
    private function addVhost(VhostApiDto $vhostDto, array $groups): bool
    {
        $storageKey = $this->getVhostStorageKey($vhostDto);

        if (!$this->redis->sismember(self::INDEX_KEY_VHOSTS, $storageKey)) {
            $this->redis->sadd(self::INDEX_KEY_VHOSTS, $storageKey);
        }

        $this->redis->hmset($storageKey, $vhostDto->toInternalData());

        $this->initLastProcessedAtKeys($storageKey, $groups);

        return true;
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return bool
     */
    public function removeVhost(VhostApiDto $vhostDto): bool
    {
        $storageKey = $this->getVhostStorageKey($vhostDto);

        if ($this->redis->sismember(self::INDEX_KEY_VHOSTS, $storageKey)) {
            $this->redis->srem(self::INDEX_KEY_VHOSTS, $storageKey);
        }

        if ($this->redis->exists($storageKey)) {
            $this->redis->del($storageKey);
        }

        return true;
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return bool
     */
    public function updateVhostLastProcessedAt(VhostApiDto $vhostDto): bool
    {
        $storageKey = $this->getVhostStorageKey($vhostDto);
        if (!$this->redis->exists($storageKey)) {
            return false;
        }

        $groupName = $vhostDto->getGroupName();
        if (null === $groupName) {
            return false;
        }

        $lastProcessedAtKey = $this->getLastProcessedAtKeyName($groupName);
        $this->redis->hset($storageKey, $lastProcessedAtKey, $vhostDto->getLastProcessedAt());

        return true;
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return string
     */
    private function getVhostStorageKey(VhostApiDto $vhostDto): string
    {
        return $this->getVhostStorageKeyPrefix() . $vhostDto->getName();
    }

    /**
     * @return string
     */
    private function getVhostStorageKeyPrefix(): string
    {
        return 'rabbitmq_vhost|';
    }

    /**
     * @param QueueApiDto $queueDto
     * @param array $groups
     * @return bool
     */
    public function indexQueue(QueueApiDto $queueDto, array $groups): bool
    {
        if ($queueDto->getMessagesReady() > 0) {
            return $this->addQueue($queueDto, $groups);
        }

        return $this->removeQueue($queueDto);
    }

    /**
     * @param QueueApiDto $queueDto
     * @param array $groups
     * @return bool
     */
    private function addQueue(QueueApiDto $queueDto, array $groups): bool
    {
        $storageKey = $this->getQueueStorageKey($queueDto);
        $indexKey = $this->getQueueIndexKey($queueDto->getVhostName());

        if (!$this->redis->sismember($indexKey, $storageKey)) {
            $this->redis->sadd($indexKey, $storageKey);
        }

        $this->redis->hmset($storageKey, $queueDto->toInternalData());

        $this->initLastProcessedAtKeys($storageKey, $groups);

        return true;
    }

    /**
     * @param QueueApiDto $queueDto
     * @return bool
     */
    public function removeQueue(QueueApiDto $queueDto): bool
    {
        $storageKey = $this->getQueueStorageKey($queueDto);
        $indexKey = $this->getQueueIndexKey($queueDto->getVhostName());

        if ($this->redis->sismember($indexKey, $storageKey)) {
            $this->redis->srem($indexKey, $storageKey);
        }

        if ($this->redis->exists($storageKey)) {
            $this->redis->del($storageKey);
        }

        return true;
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return bool
     */
    public function updateQueueLastProcessedAt(QueueApiDto $queueDto): bool
    {
        $storageKey = $this->getQueueStorageKey($queueDto);
        if (!$this->redis->exists($storageKey)) {
            return false;
        }

        $groupName = $queueDto->getGroupName();
        if (null === $groupName) {
            return false;
        }

        $lastProcessedAtKey = $this->getLastProcessedAtKeyName($groupName);

        $this->redis->hset($storageKey, $lastProcessedAtKey, $queueDto->getLastProcessedAt());

        return true;
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return string
     */
    private function getQueueStorageKey(QueueApiDto $queueDto): string
    {
        return $this->getQueueStorageKeyPrefix($queueDto->getVhostName()) . $queueDto->getName();
    }

    /**
     * @param string $vhostName
     * @return string
     */
    private function getQueueStorageKeyPrefix(string $vhostName): string
    {
        return sprintf('rabbitmq_queue|%s|', $vhostName);
    }

    /**
     * @param string $vhostName
     * @return string
     */
    private function getQueueIndexKey(string $vhostName): string
    {
        return sprintf('%s:%s', self::INDEX_KEY_QUEUES, $vhostName);
    }

    /**
     * @param string $storageKey
     * @param array $groups
     * @return bool
     */
    private function initLastProcessedAtKeys(string $storageKey, array $groups): bool
    {
        if (!$this->redis->exists($storageKey)) {
            return false;
        }

        if (empty($groups)) {
            return false;
        }

        foreach ($groups as $groupName) {
            $lastProcessedAtKey = $this->getLastProcessedAtKeyName($groupName);
            if ($this->redis->hexists($storageKey, $lastProcessedAtKey)) {
                continue;
            }

            $this->redis->hset($storageKey, $lastProcessedAtKey, 0);
        }

        return true;
    }

    /**
     * @param string $groupName
     * @return string
     */
    public function getLastProcessedAtKeyName(string $groupName): string
    {
        return sprintf('last_processed_at:%s', $groupName);
    }
}

