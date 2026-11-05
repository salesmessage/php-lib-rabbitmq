<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\VhostsConsumers;

use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Tests\Mocks\TestEncryptedJob;
use Salesmessage\LibRabbitMQ\Tests\Mocks\TestJob;
use Salesmessage\LibRabbitMQ\Tests\Support\BatchCollectingConsumer;

class AbstractVhostsConsumerStopTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_stop_requeues_the_collected_batch_once_up_to_the_last_delivery(): void
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_nack')->once()->with(3, true, true);

        $consumer = BatchCollectingConsumer::make();
        $consumer->collect(
            $this->message($channel, 1, TestJob::class),
            $this->message($channel, 3, TestEncryptedJob::class),
            $this->message($channel, 2, TestJob::class),
        );

        $this->assertSame(0, $consumer->stop(0));
        $this->assertSame(0, $consumer->stop(0));
    }

    public function test_stop_without_collected_messages_only_stops(): void
    {
        $this->assertSame(12, BatchCollectingConsumer::make()->stop(12));
    }

    public function test_stop_completes_when_the_channel_is_already_closed(): void
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_nack')->once()->andThrow(new AMQPChannelClosedException('Channel connection is closed.'));

        $consumer = BatchCollectingConsumer::make();
        $consumer->collect($this->message($channel, 1, TestJob::class));

        $this->assertSame(0, $consumer->stop(0));
    }

    private function message(AMQPChannel $channel, int $deliveryTag, string $jobClass): AMQPMessage
    {
        $message = new AMQPMessage(json_encode(['data' => ['commandName' => $jobClass]]));
        $message->setChannel($channel);
        $message->setDeliveryInfo($deliveryTag, false, '', 'queue');

        return $message;
    }
}
