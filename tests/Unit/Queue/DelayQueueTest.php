<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Queue;

use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Queue\DelayQueue;

class DelayQueueTest extends TestCase
{
    public function test_name_depends_only_on_the_delay(): void
    {
        $this->assertSame('delay.10000', DelayQueue::name(10000));
        $this->assertSame('delay.1000', DelayQueue::name(1000));
    }

    public function test_arguments_are_permanent_quorum_and_take_the_destination_from_the_message(): void
    {
        $arguments = DelayQueue::arguments(10000, 'some-exchange');

        $this->assertSame([
            'x-dead-letter-exchange' => 'some-exchange',
            'x-message-ttl' => 10000,
            'x-queue-type' => 'quorum',
        ], $arguments);
        $this->assertArrayNotHasKey('x-expires', $arguments);
        $this->assertArrayNotHasKey('x-dead-letter-routing-key', $arguments);
    }

    public function test_lock_queue_has_a_new_name_so_the_legacy_queue_with_x_expires_is_not_redeclared(): void
    {
        $name = DelayQueue::lockQueueName('some-queue', 185000);

        $this->assertSame('some-queue.dedup-lock-delay-v2.185000', $name);
        $this->assertNotSame('some-queue.dedup-lock-delay.185000', $name);
    }

    public function test_lock_queue_arguments_are_permanent_quorum_with_message_level_dead_lettering(): void
    {
        $arguments = DelayQueue::lockQueueArguments(185000, 'some-exchange', 'some-routing-key');

        $this->assertSame([
            'x-message-ttl' => 185000,
            'x-dead-letter-exchange' => 'some-exchange',
            'x-dead-letter-routing-key' => 'some-routing-key',
            'x-queue-type' => 'quorum',
        ], $arguments);
        $this->assertArrayNotHasKey('x-expires', $arguments);
    }
}
