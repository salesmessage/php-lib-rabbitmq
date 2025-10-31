<?php

namespace Salesmessage\LibRabbitMQ\Contracts;

/**
 * Each Laravel job should implement this interface.
 */
interface RabbitMQConsumable
{
    /**
     * Check duplications on the application side.
     * It's mostly represented as an idempotency checker.
     */
    public function isDuplicated(): bool;
}
