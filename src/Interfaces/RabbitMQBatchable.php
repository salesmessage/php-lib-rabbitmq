<?php

namespace Salesmessage\LibRabbitMQ\Interfaces;

interface RabbitMQBatchable
{
    /**
     * Filter out jobs that have already been processed according to the application logic.
     *
     * @param list<static> $batch
     * @return list<static>
     */
    public static function getNotDuplicatedBatchedJobs(array $batch): array;

    /**
     * Processing jobs array of static class
     *
     * @param list<static> $batch
     */
    public static function collection(array $batch): void;
}
