<?php

namespace Salesmessage\LibRabbitMQ\Exceptions;

class MessageLockedException extends \RuntimeException
{
    public function __construct()
    {
        parent::__construct('Deduplication lock is held by another consumer and action_on_lock is reject.');
    }
}
