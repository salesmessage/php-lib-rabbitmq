<?php

namespace Salesmessage\LibRabbitMQ\Tests\Feature;

use Illuminate\Support\Str;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Tests\Mocks\TestJob;
use Salesmessage\LibRabbitMQ\Tests\Support\BatchCollectingConsumer;

class StopReturnsCollectedBatchTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const MESSAGES_COUNT = 3;

    private AMQPStreamConnection $connection;

    private AMQPChannel $channel;

    private string $queue;

    protected function setUp(): void
    {
        parent::setUp();

        $this->connection = new AMQPStreamConnection(getenv('HOST'), (int) getenv('PORT'), 'guest', 'guest');
        $version = (string) ($this->connection->getServerProperties()['version'][1] ?? '0');
        if (version_compare($version, '4.3', '<')) {
            $this->connection->close();
            $this->markTestSkipped("RabbitMQ $version raises x-delivery-count on a basic.nack requeue as well; 4.3 and later do not.");
        }

        $this->channel = $this->connection->channel();
        $this->queue = 'stop-returns-collected-batch-'.Str::random(8);
        $this->channel->queue_declare($this->queue, false, true, false, false, false, new AMQPTable([
            'x-queue-type' => 'quorum',
        ]));
    }

    protected function tearDown(): void
    {
        if (isset($this->channel)) {
            $this->channel->queue_delete($this->queue);
            $this->channel->close();
            $this->connection->close();
        }

        parent::tearDown();
    }

    public function test_stopped_worker_returns_its_batch_without_raising_the_delivery_count(): void
    {
        $body = json_encode(['data' => ['commandName' => TestJob::class]]);
        $this->channel->confirm_select();
        for ($i = 0; $i < self::MESSAGES_COUNT; $i++) {
            $this->channel->basic_publish(new AMQPMessage($body), '', $this->queue);
        }
        $this->channel->wait_for_pending_acks(5);

        $collected = $this->getMessages();
        $this->assertCount(self::MESSAGES_COUNT, $collected);

        $consumer = BatchCollectingConsumer::make();
        $consumer->collect(...$collected);
        $consumer->stop(0);

        $redelivered = $this->getMessages();
        $this->assertCount(self::MESSAGES_COUNT, $redelivered);
        foreach ($redelivered as $message) {
            $this->assertTrue($message->isRedelivered());
            $this->assertArrayNotHasKey('x-delivery-count', $this->headers($message));
            $message->ack();
        }
    }

    /**
     * @return list<AMQPMessage>
     */
    private function getMessages(): array
    {
        $messages = [];
        while (count($messages) < self::MESSAGES_COUNT && $message = $this->channel->basic_get($this->queue)) {
            $messages[] = $message;
        }

        return $messages;
    }

    /**
     * @return array<string, mixed>
     */
    private function headers(AMQPMessage $message): array
    {
        $headers = $message->get_properties()['application_headers'] ?? null;

        return $headers instanceof AMQPTable ? $headers->getNativeData() : [];
    }
}
