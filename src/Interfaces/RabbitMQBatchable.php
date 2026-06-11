<?php

namespace Salesmessage\LibRabbitMQ\Interfaces;

interface RabbitMQBatchable
{
    public const BATCH_TIMEOUT = 0;

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

    /*
     * Optional timeout cleanup hook (not part of the required contract).
     *
     * If a batchable job class declares the following static method, the consumer
     * invokes it from the BATCH_TIMEOUT signal handler right before the worker is
     * killed, so the job can release locks / reset state. It is detected via
     * method_exists(), so existing implementations are unaffected:
     *
     *     public static function failedOnTimeout(array $batch, \Throwable $e): void;
     *
     * Keep the implementation fast and non-blocking: it runs inside the SIGALRM
     * handler of a process that is about to be SIGKILL'd, with no further timeout.
     */
}
