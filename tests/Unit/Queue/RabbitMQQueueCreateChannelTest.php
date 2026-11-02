<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Queue;

use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Salesmessage\LibRabbitMQ\Queue\QueueConfigFactory;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;

/**
 * When php-amqplib connects again on its own to open a channel (the vhost may have been recreated empty
 * meanwhile), the declarations made before are forgotten, so they are made again.
 */
class RabbitMQQueueCreateChannelTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_declarations_are_forgotten_when_the_connection_connects_again_to_open_a_channel(): void
    {
        $queue = $this->queueWithDeclaredQueue(connected: false);

        $queue->getChannel();

        $this->assertSame([], (new ReflectionProperty(RabbitMQQueue::class, 'queues'))->getValue($queue));
    }

    public function test_declarations_are_kept_when_the_connection_is_up(): void
    {
        $queue = $this->queueWithDeclaredQueue(connected: true);

        $queue->getChannel();

        $this->assertSame(['default'], (new ReflectionProperty(RabbitMQQueue::class, 'queues'))->getValue($queue));
    }

    private function queueWithDeclaredQueue(bool $connected): RabbitMQQueue
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('isConnected')->andReturn($connected);
        $connection->shouldReceive('channel')->andReturn(Mockery::mock(AMQPChannel::class)->shouldIgnoreMissing());
        $queue = new RabbitMQQueue(QueueConfigFactory::make([]));
        $queue->setConnection($connection);
        (new ReflectionProperty(RabbitMQQueue::class, 'queues'))->setValue($queue, ['default']);

        return $queue;
    }
}
