<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Illuminate\Redis\Connections\PredisConnection;

/**
 * Serializes access to a single Redis connection so it can be shared safely
 * between Swoole coroutines (e.g. the consumer's main loop and the
 * processing-time accrual coroutine) without multiplexing commands on one socket.
 *
 * A capacity-1 channel holds the one connection: a coroutine borrows it (pop,
 * yielding cooperatively if another coroutine holds it) and returns it (push)
 * around every command. Outside a coroutine (plain CLI, scanner, tests) there is
 * no concurrency, so commands pass straight through to the wrapped connection.
 *
 * Only the library's own storage connection is ever wrapped - never the
 * application's Redis connection, which jobs may use.
 */
class RedisConnectionPool extends PredisConnection
{
    private mixed $channel = null;

    public function __construct(private PredisConnection $connection, private int $size = 1)
    {
        // keep the pool a fully-valid PredisConnection: inherited methods operate
        // on the wrapped connection's client, while __call gates the commands
        parent::__construct($connection->client());
    }

    public function __call($method, $parameters)
    {
        $channel = $this->channel();
        if (null === $channel) {
            return $this->connection->{$method}(...$parameters);
        }

        $connection = $channel->pop();
        try {
            return $connection->{$method}(...$parameters);
        } finally {
            $channel->push($connection);
        }
    }

    /**
     * The borrow/return channel, created lazily on first use inside a coroutine.
     * Returns null when there is no coroutine scheduler (nothing to serialize).
     *
     * @return object|null
     */
    private function channel(): ?object
    {
        if (!$this->inCoroutine()) {
            return null;
        }

        if (null === $this->channel) {
            $channelClass = $this->channelClass();
            if (null === $channelClass) {
                return null;
            }

            $this->channel = new $channelClass(max(1, $this->size));
            $this->channel->push($this->connection);
        }

        return $this->channel;
    }

    private function inCoroutine(): bool
    {
        if (class_exists('\Swoole\Coroutine')) {
            return \Swoole\Coroutine::getCid() >= 0;
        }

        if (class_exists('\OpenSwoole\Coroutine')) {
            return \OpenSwoole\Coroutine::getCid() >= 0;
        }

        return false;
    }

    private function channelClass(): ?string
    {
        if (class_exists('\Swoole\Coroutine\Channel')) {
            return '\Swoole\Coroutine\Channel';
        }

        if (class_exists('\OpenSwoole\Coroutine\Channel')) {
            return '\OpenSwoole\Coroutine\Channel';
        }

        return null;
    }
}
