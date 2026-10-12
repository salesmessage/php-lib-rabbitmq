<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Support\Str;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;

class FailedJobWriter
{
    public function __construct(
        private FailedJobProviderInterface $failer,
        private LoggerInterface $logger,
    ) {
    }

    public function write(string $connectionName, string $queue, string $payload, \Throwable $exception): bool
    {
        try {
            $this->failer->log($connectionName, $queue, $this->toStorablePayload($payload), $exception);

            return true;
        } catch (\Throwable $writeException) {
            $this->logger->error('Salesmessage.LibRabbitMQ.Services.FailedJobWriter.write.exception', [
                'connection' => $connectionName,
                'queue' => $queue,
                'failure_class' => get_class($exception),
                'message' => $writeException->getMessage(),
                'trace' => $writeException->getTraceAsString(),
                'error_class' => get_class($writeException),
            ]);

            return false;
        }
    }

    /**
     * @return bool "false" when the row could not be written and the message was requeued
     */
    public function rejectMessage(
        AMQPMessage $message,
        string $connectionName,
        string $queue,
        \Throwable $exception,
        bool $ack = false,
    ): bool {
        if (!$this->write($connectionName, $queue, $message->getBody(), $exception)) {
            $message->nack(true);

            return false;
        }

        if ($ack) {
            $message->ack();
        } else {
            $message->reject(false);
        }

        return true;
    }

    /**
     * A payload the failer cannot store would be requeued forever: the database-uuids failer reads "uuid" from it,
     * and Postgres text rejects NUL bytes and invalid UTF-8.
     */
    private function toStorablePayload(string $payload): string
    {
        $decoded = json_decode($payload, true);
        if (is_array($decoded) && is_string($decoded['uuid'] ?? null)) {
            return $payload;
        }

        return json_encode(['uuid' => (string) Str::uuid(), 'body' => $payload], JSON_INVALID_UTF8_SUBSTITUTE);
    }
}
