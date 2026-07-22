<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Str;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;

/**
 * https://github.com/phpredis/phpredis
 * https://redis.io/docs/latest/commands/sort/
 */
class InternalStorageManager
{
    /**
     * @var string
     */
    private string $connectionName = 'rabbitmq_vhosts';

    /**
     * @var PredisConnection
     */
    private PredisConnection $redis;

    public function __construct() {
        /** @var PredisConnection $redis */
        $redis = Redis::connection('persistent');
        $this->redis = $redis;
    }

    /**
     * @param string $connectionName
     * @return $this
     */
    public function setConnection(string $connectionName): self
    {
        $this->connectionName = $connectionName;

        return $this;
    }

    /**
     * @return array
     */
    public function getInterimVhosts(): array
    {
        return $this->redis->hgetall($this->getInterimKeyVhosts());
    }

    /**
     * @return int
     */
    public function getInterimVhostsCount(): int
    {
        return (int) $this->redis->hlen($this->getInterimKeyVhosts());
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return void
     */
    public function addInterimVhost(VhostApiDto $vhostDto): void
    {
        $this->redis->hset($this->getInterimKeyVhosts(), $vhostDto->getName(), json_encode($vhostDto->toInternalData()));
    }

    /**
     * @param array $vhostNames
     * @return void
     */
    public function removeInterimVhost(VhostApiDto $vhostDto): void
    {
        if ('' === $vhostDto->getName()) {
            return;
        }

        $interimKeyVhosts = $this->getInterimKeyVhosts();
        if (!$this->redis->hexists($interimKeyVhosts, $vhostDto->getName())) {
            return;
        }

        $this->redis->hdel($interimKeyVhosts, [$vhostDto->getName()]);
    }

    /**
     * @param string $by
     * @param bool $alpha
     * @return array
     */
    public function getVhosts(string $by = 'name', bool $alpha = true): array
    {
        $vhosts = $this->redis->sort($this->getIndexKeyVhosts(), [
            'by' => '*->' . $by,
            'alpha' => $alpha,
            'sort' => 'asc',
        ]);

        return array_map(fn($value): string => Str::replaceFirst(
            $this->getVhostStorageKeyPrefix(),
            '',
            $value
        ), $vhosts);
    }

    /**
     * Ordered vhosts together with the numeric weight they were sorted by.
     * Returns a list of ['name' => string, 'weight' => float] ascending by weight.
     * A missing weight field is treated as 0.
     *
     * @param string $by
     * @return array
     */
    public function getVhostsWithWeights(string $by): array
    {
        $rows = $this->redis->sort($this->getIndexKeyVhosts(), [
            'by' => '*->' . $by,
            'get' => ['#', '*->' . $by],
            'alpha' => false,
            'sort' => 'asc',
        ]);

        $prefix = $this->getVhostStorageKeyPrefix();

        $result = [];
        for ($i = 0, $len = count($rows); $i < $len; $i += 2) {
            $result[] = [
                'name' => Str::replaceFirst($prefix, '', (string) $rows[$i]),
                'weight' => (float) ($rows[$i + 1] ?? 0),
            ];
        }

        return $result;
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

        return array_map(fn($value): string => Str::replaceFirst(
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
        $isAddVhost = ($vhostDto->getMessagesReady() > 0) || ($vhostDto->getMessagesUnacknowledged() > 0);
        if ($isAddVhost) {
            $this->addVhost($vhostDto, $groups);
        } else {
            $this->removeVhost($vhostDto);
        }

        return $isAddVhost;
    }

    /**
     * @param VhostApiDto $vhostDto
     * @param array $groups
     * @return void
     */
    private function addVhost(VhostApiDto $vhostDto, array $groups): void
    {
        $storageKey = $this->getVhostStorageKey($vhostDto);

        $indexKeyVhosts = $this->getIndexKeyVhosts();
        if (!$this->redis->sismember($indexKeyVhosts, $storageKey)) {
            $this->redis->sadd($indexKeyVhosts, $storageKey);
        }

        $this->redis->hmset($storageKey, $vhostDto->toInternalData());

        $this->initLastProcessedAtKeys($storageKey, $groups);
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return void
     */
    public function removeVhost(VhostApiDto $vhostDto): void
    {
        $storageKey = $this->getVhostStorageKey($vhostDto);

        $indexKeyVhosts = $this->getIndexKeyVhosts();
        if ($this->redis->sismember($indexKeyVhosts, $storageKey)) {
            $this->redis->srem($indexKeyVhosts, $storageKey);
        }

        if ($this->redis->exists($storageKey)) {
            $this->redis->del($storageKey);
        }
    }

    /**
     * @param VhostApiDto $vhostDto
     * @param array $groups
     * @return bool
     */
    public function activateVhost(VhostApiDto $vhostDto, array $groups): bool
    {
        $storageKey = $this->getVhostStorageKey($vhostDto);

        $messages = (int) $this->redis->hget($storageKey, 'messages') + 1;
        $vhostDto->setMessages($messages);

        $messagesReady = (int) $this->redis->hget($storageKey, 'messages_ready') + 1;
        $vhostDto->setMessagesReady($messagesReady);

        $this->addVhost($vhostDto, $groups);

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
        if ($this->isVhostsConnection()) {
            return 'rabbitmq_vhost|';
        }

        return $this->connectionName . '_vhost|';
    }

    /**
     * @param QueueApiDto $queueDto
     * @param array $groups
     * @return bool
     */
    public function indexQueue(QueueApiDto $queueDto, array $groups): bool
    {
        $isAddQueue = ($queueDto->getMessagesReady() > 0) || ($queueDto->getMessagesUnacknowledged() > 0);
        if ($isAddQueue) {
            $this->addQueue($queueDto, $groups);
        } else {
            $this->removeQueue($queueDto);
        }

        return $isAddQueue;
    }

    /**
     * @param QueueApiDto $queueDto
     * @param array $groups
     * @return void
     */
    private function addQueue(QueueApiDto $queueDto, array $groups): void
    {
        $storageKey = $this->getQueueStorageKey($queueDto);
        $indexKey = $this->getQueueIndexKey($queueDto->getVhostName());

        if (!$this->redis->sismember($indexKey, $storageKey)) {
            $this->redis->sadd($indexKey, $storageKey);
        }

        $this->redis->hmset($storageKey, $queueDto->toInternalData());

        $this->initLastProcessedAtKeys($storageKey, $groups);
    }

    /**
     * @param QueueApiDto $queueDto
     * @return void
     */
    public function removeQueue(QueueApiDto $queueDto): void
    {
        $storageKey = $this->getQueueStorageKey($queueDto);
        $indexKey = $this->getQueueIndexKey($queueDto->getVhostName());

        if ($this->redis->sismember($indexKey, $storageKey)) {
            $this->redis->srem($indexKey, $storageKey);
        }

        if ($this->redis->exists($storageKey)) {
            $this->redis->del($storageKey);
        }
    }

    /**
     * @param QueueApiDto $queueDto
     * @param array $groups
     * @return bool
     */
    public function activateQueue(QueueApiDto $queueDto, array $groups): bool
    {
        $storageKey = $this->getQueueStorageKey($queueDto);

        $messages = (int) $this->redis->hget($storageKey, 'messages') + 1;
        $queueDto->setMessages($messages);

        $messagesReady = (int) $this->redis->hget($storageKey, 'messages_ready') + 1;
        $queueDto->setMessagesReady($messagesReady);

        $this->addQueue($queueDto, $groups);

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
     * @param QueueApiDto $queueDto
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
        $queueKey = $this->isVhostsConnection()
            ? 'rabbitmq_queue'
            : $this->connectionName . '_queue';

        return sprintf('%s|%s|', $queueKey, $vhostName);
    }

    /**
     * @param string $vhostName
     * @return string
     */
    private function getQueueIndexKey(string $vhostName): string
    {
        return sprintf('%s:%s', $this->getIndexKeyQueues(), $vhostName);
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

    /**
     * Update the last_processed_at timestamp for a vhost and one of its queues
     * within a group.
     *
     * @param string $group
     * @param string $vhost
     * @param string $queue
     * @return void
     */
    public function touchLastProcessedAt(string $group, string $vhost, string $queue): void
    {
        $timestamp = time();

        $queueDto = new QueueApiDto([
            'name' => $queue,
            'vhost' => $vhost,
        ]);
        $queueDto
            ->setGroupName($group)
            ->setLastProcessedAt($timestamp);
        $this->updateQueueLastProcessedAt($queueDto);

        $vhostDto = new VhostApiDto([
            'name' => $vhost,
        ]);
        $vhostDto
            ->setGroupName($group)
            ->setLastProcessedAt($timestamp);
        $this->updateVhostLastProcessedAt($vhostDto);
    }

    /**
     * Hash field on a vhost holding its processing time (integer milliseconds)
     * within the sliding window for a group. Used as the sort key in
     * processing_time mode.
     *
     * @param string $groupName
     * @return string
     */
    public function getWindowCostKeyName(string $groupName): string
    {
        return sprintf('window_cost:%s', $groupName);
    }

    /**
     * Lua body shared by recordProcessingTime() and refreshWindowCost(): trim
     * expired buckets, sum the live ones, clamp to >= 0 and materialize the cost
     * on the vhost hash - all in one atomic script, so concurrent workers and
     * scanners cannot clobber each other with a stale sum, nor resurrect a
     * just-deleted vhost key between the existence check and the write.
     *
     * KEYS[1] buckets hash, KEYS[2] vhost storage hash.
     * ARGV[1] oldest valid bucket id, ARGV[2] window cost field.
     */
    private const LUA_REFRESH_WINDOW_COST = <<<'LUA'
local buckets = redis.call('HGETALL', KEYS[1])
local oldest = tonumber(ARGV[1])
local cost = 0
for i = 1, #buckets, 2 do
    if (tonumber(buckets[i]) or 0) < oldest then
        redis.call('HDEL', KEYS[1], buckets[i])
    else
        cost = cost + (tonumber(buckets[i + 1]) or 0)
    end
end
-- buckets may sum below zero when a provisional charge expired before its
-- refund/reconciliation; a vhost cannot cost less than zero
if cost < 0 then
    cost = 0
end
if cost > 0 then
    if redis.call('EXISTS', KEYS[2]) == 1 then
        redis.call('HSET', KEYS[2], ARGV[2], cost)
    end
else
    redis.call('HDEL', KEYS[2], ARGV[2])
end
return cost
LUA;

    /**
     * Add processing time (integer milliseconds) to the current sliding-window
     * bucket of a vhost and refresh the materialized window cost atomically. The
     * amount may be negative to reconcile a previously added provisional
     * (reservation) estimate. Integer math keeps reconciliation exact and allows
     * HINCRBY.
     *
     * @param string $group
     * @param string $vhost
     * @param int $milliseconds
     * @param int $window
     * @param int $bucket
     * @return void
     */
    public function recordProcessingTime(
        string $group,
        string $vhost,
        int $milliseconds,
        int $window,
        int $bucket
    ): void {
        $now = time();

        $this->evalLua(
            $this->recordProcessingScript(),
            [
                $this->getProcessingBucketsKey($group, $vhost),
                $this->getVhostStorageKeyPrefix() . $vhost,
            ],
            [
                intdiv($now - $window, $bucket),
                $this->getWindowCostKeyName($group),
                intdiv($now, $bucket),
                $milliseconds,
                $window + $bucket,
            ]
        );
    }

    /**
     * Recompute a vhost's window cost (integer milliseconds) from its live
     * buckets (trimming expired ones) and store it on the vhost hash atomically.
     * Returns the recomputed cost.
     *
     * @param string $group
     * @param string $vhost
     * @param int $window
     * @param int $bucket
     * @return int
     */
    public function refreshWindowCost(
        string $group,
        string $vhost,
        int $window,
        int $bucket
    ): int {
        return (int) $this->evalLua(
            self::LUA_REFRESH_WINDOW_COST,
            [
                $this->getProcessingBucketsKey($group, $vhost),
                $this->getVhostStorageKeyPrefix() . $vhost,
            ],
            [
                intdiv(time() - $window, $bucket),
                $this->getWindowCostKeyName($group),
            ]
        );
    }

    /**
     * recordProcessingTime()'s Lua: charge the current bucket and refresh its
     * TTL, then run the shared refresh body.
     *
     * KEYS[1] buckets hash, KEYS[2] vhost storage hash.
     * ARGV[1] oldest valid bucket id, ARGV[2] window cost field,
     * ARGV[3] current bucket id, ARGV[4] milliseconds, ARGV[5] buckets ttl.
     *
     * @return string
     */
    private function recordProcessingScript(): string
    {
        return "redis.call('HINCRBY', KEYS[1], ARGV[3], ARGV[4])\n"
            . "redis.call('EXPIRE', KEYS[1], ARGV[5])\n"
            . self::LUA_REFRESH_WINDOW_COST;
    }

    /**
     * Run a Lua script atomically, preferring EVALSHA and falling back to EVAL
     * the first time a script is not yet cached on the server.
     *
     * @param string $script
     * @param array $keys
     * @param array $arguments
     * @return mixed
     */
    private function evalLua(string $script, array $keys, array $arguments): mixed
    {
        $parameters = array_merge($keys, $arguments);
        $numKeys = count($keys);

        try {
            return $this->redis->evalsha(sha1($script), $numKeys, ...$parameters);
        } catch (\Throwable $exception) {
            if (!str_contains(strtoupper($exception->getMessage()), 'NOSCRIPT')) {
                throw $exception;
            }

            return $this->redis->eval($script, $numKeys, ...$parameters);
        }
    }

    /**
     * @param string $group
     * @param string $vhost
     * @return string
     */
    private function getProcessingBucketsKey(string $group, string $vhost): string
    {
        $prefix = $this->isVhostsConnection() ? 'rabbitmq' : $this->connectionName;

        return sprintf('%s_proc_buckets|%s|%s', $prefix, $group, $vhost);
    }

    /**
     * @return string
     */
    private function getInterimKeyVhosts(): string
    {
        if ($this->isVhostsConnection()) {
            return 'rabbitmq_interim_vhosts';
        }

        return $this->connectionName . '_interim_vhosts';
    }

    /**
     * @return string
     */
    private function getIndexKeyVhosts(): string
    {
        if ($this->isVhostsConnection()) {
            return 'rabbitmq_vhosts_index';
        }

        return $this->connectionName . '_vhosts_index';
    }

    /**
     * @return string
     */
    private function getIndexKeyQueues(): string
    {
        if ($this->isVhostsConnection()) {
            return 'rabbitmq_queues_index';
        }

        return $this->connectionName . '_queues_index';
    }

    /**
     * @return bool
     */
    private function isVhostsConnection(): bool
    {
        return 'rabbitmq_vhosts' === $this->connectionName;
    }
}

