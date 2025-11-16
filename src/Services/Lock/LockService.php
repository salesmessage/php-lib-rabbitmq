<?php

namespace Salesmessage\LibRabbitMQ\Services\Lock;

use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Support\Sleep;

class LockService
{
    private const SLEEP_MS = 250;

    public function __construct(
        private LockProvider $lockProvider,
    ) {}

    public function lock(
        string $lockKey,
        \Closure $handler,
        int $lockSec = 10,
        int $waitForLockSec = 11,
        bool $skipHandlingOnLock = false,
    ): bool {
        $hadLock = false;
        $starting = ((int) now()->format('Uu')) / 1000;
        $milliseconds = $waitForLockSec * 1000;
        $lock = $this->lockProvider->lock($lockKey, $lockSec);

        /** logic was taken from @see \Illuminate\Cache\Lock::block */
        while (!$lock->acquire()) {
            $now = ((int) now()->format('Uu')) / 1000;
            if (($now + self::SLEEP_MS - $milliseconds) >= $starting) {
                throw new LockTimeoutException;
            }
            Sleep::usleep(self::SLEEP_MS * 1000);
            $hadLock = true;
        }

        try {
            if ($skipHandlingOnLock && $hadLock) {
                return false;
            }

            $handler();
        } finally {
            $lock->release();
        }

        return true;
    }
}
