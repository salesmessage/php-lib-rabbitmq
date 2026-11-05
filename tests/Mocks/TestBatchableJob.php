<?php

namespace Salesmessage\LibRabbitMQ\Tests\Mocks;

use Salesmessage\LibRabbitMQ\Interfaces\RabbitMQBatchable;

class TestBatchableJob implements RabbitMQBatchable
{
    public static function getNotDuplicatedBatchedJobs(array $batch): array
    {
        return $batch;
    }

    public static function collection(array $batch): void {}
}
