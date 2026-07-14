<?php

namespace Salesmessage\LibRabbitMQ\Tests\Support;

use Illuminate\Redis\Connections\PredisConnection;

/**
 * In-memory stand-in for the Predis connection used by InternalStorageManager.
 * Implements real semantics for the commands the storage relies on, including
 * SORT with BY hash patterns and GET, so storage + scheduler behavior can be
 * tested end-to-end without a Redis server.
 */
class FakeRedisConnection extends PredisConnection
{
    /** @var array<string, array<string, string>> */
    public array $hashes = [];

    /** @var array<string, array<int|string, string>> */
    public array $sets = [];

    /** @var array<string, int> */
    public array $ttls = [];

    public function __construct()
    {
    }

    public function hset($key, $field, $value): int
    {
        $isNew = !isset($this->hashes[$key][$field]);
        $this->hashes[$key][$field] = (string) $value;

        return $isNew ? 1 : 0;
    }

    public function hget($key, $field): ?string
    {
        return $this->hashes[$key][$field] ?? null;
    }

    public function hmset($key, array $dictionary): bool
    {
        foreach ($dictionary as $field => $value) {
            $this->hashes[$key][$field] = (string) $value;
        }

        return true;
    }

    public function hgetall($key): array
    {
        return $this->hashes[$key] ?? [];
    }

    public function hlen($key): int
    {
        return count($this->hashes[$key] ?? []);
    }

    public function hexists($key, $field): int
    {
        return isset($this->hashes[$key][$field]) ? 1 : 0;
    }

    public function hdel($key, $fields): int
    {
        $removed = 0;
        foreach ((array) $fields as $field) {
            if (isset($this->hashes[$key][$field])) {
                unset($this->hashes[$key][$field]);
                $removed++;
            }
        }

        return $removed;
    }

    public function hincrby($key, $field, $increment): int
    {
        $value = (int) ($this->hashes[$key][$field] ?? 0) + (int) $increment;
        $this->hashes[$key][$field] = (string) $value;

        return $value;
    }

    public function sadd($key, $members): int
    {
        $added = 0;
        foreach ((array) $members as $member) {
            if (!in_array((string) $member, $this->sets[$key] ?? [], true)) {
                $this->sets[$key][] = (string) $member;
                $added++;
            }
        }

        return $added;
    }

    public function srem($key, $members): int
    {
        $removed = 0;
        foreach ((array) $members as $member) {
            $index = array_search((string) $member, $this->sets[$key] ?? [], true);
            if (false !== $index) {
                unset($this->sets[$key][$index]);
                $removed++;
            }
        }

        return $removed;
    }

    public function sismember($key, $member): int
    {
        return in_array((string) $member, $this->sets[$key] ?? [], true) ? 1 : 0;
    }

    public function exists($key): int
    {
        return (isset($this->hashes[$key]) || isset($this->sets[$key])) ? 1 : 0;
    }

    public function del($keys): int
    {
        $removed = 0;
        foreach ((array) $keys as $key) {
            if (isset($this->hashes[$key]) || isset($this->sets[$key])) {
                unset($this->hashes[$key], $this->sets[$key], $this->ttls[$key]);
                $removed++;
            }
        }

        return $removed;
    }

    public function expire($key, $seconds): int
    {
        if (!$this->exists($key)) {
            return 0;
        }

        $this->ttls[$key] = (int) $seconds;

        return 1;
    }

    public function ttl($key): int
    {
        if (!$this->exists($key)) {
            return -2;
        }

        return $this->ttls[$key] ?? -1;
    }

    /**
     * SORT with the subset of options the storage uses:
     * by ('*->field'), alpha, sort (asc/desc), get (['#', '*->field']).
     */
    public function sort($key, array $options = []): array
    {
        $members = array_values($this->sets[$key] ?? []);

        $by = (string) ($options['by'] ?? '');
        $alpha = (bool) ($options['alpha'] ?? false);

        $weightOf = function (string $member) use ($by): ?string {
            if ('' === $by || !str_starts_with($by, '*->')) {
                return $member;
            }

            return $this->hashes[$member][substr($by, 3)] ?? null;
        };

        usort($members, function (string $a, string $b) use ($weightOf, $alpha): int {
            if ($alpha) {
                return strcmp((string) $weightOf($a), (string) $weightOf($b));
            }

            return ((float) $weightOf($a)) <=> ((float) $weightOf($b));
        });

        if ('desc' === strtolower((string) ($options['sort'] ?? 'asc'))) {
            $members = array_reverse($members);
        }

        $getPatterns = (array) ($options['get'] ?? []);
        if (empty($getPatterns)) {
            return $members;
        }

        $result = [];
        foreach ($members as $member) {
            foreach ($getPatterns as $pattern) {
                if ('#' === $pattern) {
                    $result[] = $member;
                } elseif (str_starts_with((string) $pattern, '*->')) {
                    $result[] = $this->hashes[$member][substr((string) $pattern, 3)] ?? null;
                }
            }
        }

        return $result;
    }
}
