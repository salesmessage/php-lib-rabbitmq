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

    public function test_arguments_are_quorum_expire_when_idle_and_take_the_destination_from_the_message(): void
    {
        $arguments = DelayQueue::arguments(10000, 'some-exchange');

        $this->assertSame([
            'x-dead-letter-exchange' => 'some-exchange',
            'x-expires' => 10000 + 86400000,
            'x-message-ttl' => 10000,
            'x-queue-type' => 'quorum',
        ], $arguments);
        $this->assertArrayNotHasKey('x-dead-letter-routing-key', $arguments);
    }

    public function test_idle_expiry_is_always_longer_than_the_message_ttl(): void
    {
        foreach ([1000, 10000, 900000] as $ttlMs) {
            $arguments = DelayQueue::arguments($ttlMs, 'some-exchange');

            $this->assertGreaterThan($arguments['x-message-ttl'], $arguments['x-expires']);
            $this->assertSame($ttlMs + 86400000, $arguments['x-expires']);
        }
    }
}
