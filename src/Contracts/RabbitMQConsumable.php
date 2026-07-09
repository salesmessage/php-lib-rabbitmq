<?php

namespace Salesmessage\LibRabbitMQ\Contracts;

/**
 * Each Laravel job should implement this interface.
 *
 * A job may additionally define `shouldConfirmOnPublish(): bool` to opt into publisher
 * confirms. It's intentionally not part of this interface: it's detected via
 * method_exists(), so jobs that don't implement it keep the fire-and-forget publish path.
 */
interface RabbitMQConsumable
{
    public const MQ_TYPE_CLASSIC = 'classic';
    public const MQ_TYPE_QUORUM = 'quorum';

    /**
     * Check duplications on the application side.
     * It's mostly represented as an idempotency checker.
     */
    public function isDuplicated(): bool;

    public function getQueueType(): string;
}
