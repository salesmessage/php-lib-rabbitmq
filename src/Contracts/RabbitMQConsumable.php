<?php

namespace Salesmessage\LibRabbitMQ\Contracts;

/**
 * Each Laravel job should implement this interface.
 */
interface RabbitMQConsumable
{
    /**
     * Used to track the job's execution (e.g. for logging).
     *
     * @return string
     */
    public function getConsumableId(): string;

    /**
     * Check duplications on the application side.
     * It's mostly represented as an idempotency checker.
     */
    public function isDuplicated(): bool;
}
