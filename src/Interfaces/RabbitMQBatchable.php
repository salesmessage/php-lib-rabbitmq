<?php

namespace Salesmessage\LibRabbitMQ\Interfaces;

interface RabbitMQBatchable
{
    /**
     * Processing jobs array of static class
     *
     * @param array<static> $batch
     * @return mixed
     */
    public static function collection(array $batch): void;
}
