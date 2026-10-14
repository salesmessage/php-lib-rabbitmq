<?php

namespace Salesmessage\LibRabbitMQ\Exceptions;

class LockRequeueLimitReachedException extends \RuntimeException
{
    public function __construct(int $attempts, int $maxAttempts)
    {
        parent::__construct(sprintf(
            'Deduplication lock requeue limit reached: attempt %d, max attempts %d.',
            $attempts,
            $maxAttempts
        ));
    }
}
