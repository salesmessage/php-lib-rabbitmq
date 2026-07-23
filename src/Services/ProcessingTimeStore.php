<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Str;

/**
 * Sliding-window processing-time accounting shared by every entity scheduled on
 * fair processing time - vhosts and queues alike. Callers supply the Redis keys
 * for an entity; this store owns the bucket math, the atomic Lua and the
 * weight-ordered read, and does not know what the entity is. That keeps the
 * mechanism in one place while key naming stays with the storage manager.
 */
class ProcessingTimeStore
{
    /**
     * Trim expired buckets, sum the live ones, clamp to >= 0 and materialize the
     * cost on the owner hash - all in one atomic script, so concurrent workers
     * and scanners cannot clobber each other with a stale sum, nor resurrect a
     * just-deleted owner key between the existence check and the write.
     *
     * KEYS[1] buckets hash, KEYS[2] owner hash.
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
-- refund/reconciliation; an entity cannot cost less than zero
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

    public function __construct(private PredisConnection $redis)
    {
    }

    /**
     * Point the store at a different connection (used when the consumer swaps in
     * a pool-guarded connection for its coroutines).
     *
     * @param PredisConnection $redis
     * @return void
     */
    public function setConnection(PredisConnection $redis): void
    {
        $this->redis = $redis;
    }

    /**
     * Add processing time (integer milliseconds, may be negative to reconcile a
     * provisional charge) to an entity's buckets hash and refresh its
     * materialized window cost atomically.
     *
     * @param string $bucketsKey
     * @param string $ownerKey
     * @param string $windowCostField
     * @param int $milliseconds
     * @param int $window
     * @param int $bucket
     * @return void
     */
    public function record(
        string $bucketsKey,
        string $ownerKey,
        string $windowCostField,
        int $milliseconds,
        int $window,
        int $bucket
    ): void {
        $now = time();

        $this->evalLua(
            $this->recordScript(),
            [$bucketsKey, $ownerKey],
            [
                intdiv($now - $window, $bucket),
                $windowCostField,
                intdiv($now, $bucket),
                $milliseconds,
                $window + $bucket,
            ]
        );
    }

    /**
     * Recompute an entity's live window cost (trimming expired buckets), clamp to
     * >= 0 and materialize it on the owner hash atomically. Returns the cost.
     *
     * @param string $bucketsKey
     * @param string $ownerKey
     * @param string $windowCostField
     * @param int $window
     * @param int $bucket
     * @return int
     */
    public function refresh(
        string $bucketsKey,
        string $ownerKey,
        string $windowCostField,
        int $window,
        int $bucket
    ): int {
        return (int) $this->evalLua(
            self::LUA_REFRESH_WINDOW_COST,
            [$bucketsKey, $ownerKey],
            [
                intdiv(time() - $window, $bucket),
                $windowCostField,
            ]
        );
    }

    /**
     * Batched refresh(): recompute many entities' window costs in a single
     * pipelined round trip instead of one EVAL round trip per entity. The
     * script is loaded once via SCRIPT LOAD so the pipeline can use EVALSHA.
     *
     * @param array $entries list of [bucketsKey, ownerKey, windowCostField]
     * @param int $window
     * @param int $bucket
     * @return void
     */
    public function refreshMany(array $entries, int $window, int $bucket): void
    {
        if (empty($entries)) {
            return;
        }

        $script = self::LUA_REFRESH_WINDOW_COST;
        $sha = sha1($script);
        $oldestBucket = intdiv(time() - $window, $bucket);

        $this->redis->script('load', $script);

        try {
            // the closure must only queue commands on $pipe: through the
            // coroutine connection pool the whole pipeline() call runs within a
            // single borrow of the connection, and any nested pool call from
            // inside the closure would self-deadlock on the capacity-1 channel
            $this->redis->pipeline(function ($pipe) use ($entries, $sha, $oldestBucket) {
                foreach ($entries as [$bucketsKey, $ownerKey, $windowCostField]) {
                    $pipe->evalsha($sha, 2, $bucketsKey, $ownerKey, $oldestBucket, $windowCostField);
                }
            });
        } catch (\Throwable $exception) {
            if (!str_contains(strtoupper($exception->getMessage()), 'NOSCRIPT')) {
                throw $exception;
            }

            // the script cache was flushed between the load and the pipeline
            // (e.g. server restart): refresh() re-runs each entry with its own
            // EVAL fallback; refreshes are idempotent recomputations, so
            // repeating entries that already ran in the pipeline is harmless
            foreach ($entries as [$bucketsKey, $ownerKey, $windowCostField]) {
                $this->refresh($bucketsKey, $ownerKey, $windowCostField, $window, $bucket);
            }
        }
    }

    /**
     * SORT an index by an owner-hash field and return a list of
     * ['name' => string, 'weight' => float] ascending by weight, stripping
     * $prefix from each member. A missing field is treated as 0.
     *
     * @param string $indexKey
     * @param string $prefix
     * @param string $by
     * @return array
     */
    public function orderedByWeight(string $indexKey, string $prefix, string $by): array
    {
        $rows = $this->redis->sort($indexKey, [
            'by' => '*->' . $by,
            'get' => ['#', '*->' . $by],
            'alpha' => false,
            'sort' => 'asc',
        ]);

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
     * record()'s Lua: charge the current bucket and refresh its TTL, then run the
     * shared refresh body.
     *
     * KEYS[1] buckets hash, KEYS[2] owner hash.
     * ARGV[1] oldest valid bucket id, ARGV[2] window cost field,
     * ARGV[3] current bucket id, ARGV[4] milliseconds, ARGV[5] buckets ttl.
     *
     * @return string
     */
    private function recordScript(): string
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
}
