<?php

namespace Salesmessage\LibRabbitMQ\Tests\Support;

use PhpAmqpLib\Message\AMQPMessage;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;

trait RecordsProcessedBatches
{
    /** @var list<list<int>> */
    public array $processedBatches = [];

    protected function processBatch(RabbitMQQueue $connection): void
    {
        if (empty($this->batchMessages)) {
            return;
        }

        $tags = array_map(
            static fn (AMQPMessage $message): int => $message->getDeliveryTag(),
            array_merge(...array_values($this->batchMessages))
        );
        sort($tags);

        $this->processedBatches[] = $tags;
        $this->batchMessages = [];
    }
}
