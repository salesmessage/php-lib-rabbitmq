<?php

namespace Salesmessage\LibRabbitMQ\Interfaces;

interface RabbitMQBatchable
{
    /**
     * @warning Batch item (job) MUST BE IDEMPOTENT
     * since in case of failure, batch will be iterated and re-processed as separate jobs.
     * @see \Salesmessage\LibRabbitMQ\VhostsConsumers\AbstractVhostsConsumer::processBatch
     *
     * Processing jobs array of static class
     *
     * @param array<static> $batch
     * @return mixed
     */
    public static function collection(array $batch): void;
}
