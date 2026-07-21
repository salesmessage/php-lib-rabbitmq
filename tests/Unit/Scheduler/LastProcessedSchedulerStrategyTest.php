<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Scheduler;

use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Scheduler\LastProcessedSchedulerStrategy;

class LastProcessedSchedulerStrategyTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function testOrdersVhostsByLastProcessedAt(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('getLastProcessedAtKeyName')->with('billing')->andReturn('last_processed_at:billing');
        $storage->shouldReceive('getVhosts')->once()->with('last_processed_at:billing', false)->andReturn(['vhost_a', 'vhost_b']);

        $scheduler = new LastProcessedSchedulerStrategy($storage);

        $this->assertSame(['vhost_a', 'vhost_b'], $scheduler->getOrderedVhosts('billing'));
    }

    public function testOrdersQueuesByLastProcessedAt(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('getLastProcessedAtKeyName')->with('billing')->andReturn('last_processed_at:billing');
        $storage->shouldReceive('getVhostQueues')->once()->with('vhost_a', 'last_processed_at:billing', false)->andReturn(['q1', 'q2']);

        $scheduler = new LastProcessedSchedulerStrategy($storage);

        $this->assertSame(['q1', 'q2'], $scheduler->getOrderedQueues('billing', 'vhost_a'));
    }

    public function testReserveTouchesLastProcessedAt(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('touchLastProcessedAt')->once()->with('billing', 'vhost_a', 'q1');
        $storage->shouldNotReceive('recordProcessingTime');

        (new LastProcessedSchedulerStrategy($storage))->reserve('billing', 'vhost_a', 'q1');
    }

    public function testRecordTouchesLastProcessedAtAndIgnoresDuration(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('touchLastProcessedAt')->once()->with('billing', 'vhost_a', 'q1');
        $storage->shouldNotReceive('recordProcessingTime');

        (new LastProcessedSchedulerStrategy($storage))->record('billing', 'vhost_a', 'q1', 12500);
    }
}
