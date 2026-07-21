<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Scheduler;

use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Scheduler\ProcessingTimeSchedulerStrategy;

class ProcessingTimeSchedulerStrategyTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function testOrdersVhostsByWindowCostAscending(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('getWindowCostKeyName')->with('billing')->andReturn('window_cost:billing');
        $storage->shouldReceive('getVhostsWithWeights')->once()->with('window_cost:billing')->andReturn([
            ['name' => 'idle_vhost', 'weight' => 0.0],
            ['name' => 'medium_vhost', 'weight' => 12.0],
            ['name' => 'busy_vhost', 'weight' => 300.0],
        ]);

        $scheduler = new ProcessingTimeSchedulerStrategy($storage);

        $this->assertSame(['idle_vhost', 'medium_vhost', 'busy_vhost'], $scheduler->getOrderedVhosts('billing'));
    }

    public function testEqualCostVhostsAreShuffledButRemainComplete(): void
    {
        $names = ['a', 'b', 'c', 'd', 'e'];
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('getWindowCostKeyName')->andReturn('window_cost:billing');
        $storage->shouldReceive('getVhostsWithWeights')->andReturn(
            array_map(fn ($n) => ['name' => $n, 'weight' => 0.0], $names)
        );

        $scheduler = new ProcessingTimeSchedulerStrategy($storage);

        $ordered = $scheduler->getOrderedVhosts('billing');

        sort($ordered);
        $this->assertSame($names, $ordered, 'all equal-cost vhosts must still be present');
    }

    public function testHigherCostVhostsKeepTheirRelativeOrderAcrossTies(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('getWindowCostKeyName')->andReturn('window_cost:billing');
        $storage->shouldReceive('getVhostsWithWeights')->andReturn([
            ['name' => 'zero_a', 'weight' => 0.0],
            ['name' => 'zero_b', 'weight' => 0.0],
            ['name' => 'heavy', 'weight' => 99.0],
        ]);

        $scheduler = new ProcessingTimeSchedulerStrategy($storage);

        $ordered = $scheduler->getOrderedVhosts('billing');

        $this->assertSame('heavy', end($ordered), 'the heaviest vhost must always sort last');
        $this->assertContains($ordered[0], ['zero_a', 'zero_b']);
    }

    public function testOrdersQueuesByLastProcessedAt(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('getLastProcessedAtKeyName')->with('billing')->andReturn('last_processed_at:billing');
        $storage->shouldReceive('getVhostQueues')->once()->with('vhost_a', 'last_processed_at:billing', false)->andReturn(['q1', 'q2']);

        $scheduler = new ProcessingTimeSchedulerStrategy($storage);

        $this->assertSame(['q1', 'q2'], $scheduler->getOrderedQueues('billing', 'vhost_a'));
    }

    public function testReserveChargesProvisionalCost(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('touchLastProcessedAt')->once()->with('billing', 'vhost_a', 'q1');
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', 5000, 300, 30);

        $scheduler = new ProcessingTimeSchedulerStrategy($storage, ['window' => 300, 'bucket' => 30]);
        $scheduler->reserve('billing', 'vhost_a', 'q1');
    }

    public function testRecordReconcilesProvisionalFromReserve(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('touchLastProcessedAt');
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', 5000, 300, 30); // reserve
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', -800, 300, 30); // record reconciles 4200 - 5000

        $scheduler = new ProcessingTimeSchedulerStrategy($storage);
        $scheduler->reserve('billing', 'vhost_a', 'q1');
        $scheduler->record('billing', 'vhost_a', 'q1', 4200);
    }

    public function testSecondRecordAfterReserveAddsFullDuration(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('touchLastProcessedAt');
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', 5000, 300, 30); // reserve
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', -3000, 300, 30); // first record: 2000 - 5000
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', 2000, 300, 30); // second record: provisional consumed

        $scheduler = new ProcessingTimeSchedulerStrategy($storage);
        $scheduler->reserve('billing', 'vhost_a', 'q1');
        $scheduler->record('billing', 'vhost_a', 'q1', 2000);
        $scheduler->record('billing', 'vhost_a', 'q1', 2000);
    }

    public function testAbandonedReservationIsRefundedOnNextReserve(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('touchLastProcessedAt');
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', 5000, 300, 30); // reserve A
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', -5000, 300, 30); // refund A
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_b', 5000, 300, 30); // reserve B

        $scheduler = new ProcessingTimeSchedulerStrategy($storage);
        $scheduler->reserve('billing', 'vhost_a', 'q1'); // queue turned out empty, no record
        $scheduler->reserve('billing', 'vhost_b', 'q1');
    }

    public function testReReservingSameVhostDoesNotStackProvisional(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('touchLastProcessedAt');
        $storage->shouldReceive('recordProcessingTime')->twice()->with('billing', 'vhost_a', 5000, 300, 30);
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', -5000, 300, 30);
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', -3000, 300, 30); // record 2000 - 5000

        $scheduler = new ProcessingTimeSchedulerStrategy($storage);
        $scheduler->reserve('billing', 'vhost_a', 'q1');
        $scheduler->reserve('billing', 'vhost_a', 'q1'); // retry: refund then re-charge, net one provisional
        $scheduler->record('billing', 'vhost_a', 'q1', 2000);
    }

    public function testProvisionalIsFlooredToOneSecond(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('touchLastProcessedAt');
        // reservation_estimate=0 is floored to the 1000ms minimum
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', 1000, 300, 30); // reserve
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', 3200, 300, 30); // record 4200 - 1000

        $scheduler = new ProcessingTimeSchedulerStrategy($storage, ['reservation_estimate' => 0]);
        $scheduler->reserve('billing', 'vhost_a', 'q1');
        $scheduler->record('billing', 'vhost_a', 'q1', 4200);
    }

    public function testFractionalReservationEstimateConvertsToMilliseconds(): void
    {
        $storage = Mockery::mock(InternalStorageManager::class);
        $storage->shouldReceive('touchLastProcessedAt');
        $storage->shouldReceive('recordProcessingTime')->once()->with('billing', 'vhost_a', 2500, 300, 30);

        $scheduler = new ProcessingTimeSchedulerStrategy($storage, ['reservation_estimate' => 2.5]);
        $scheduler->reserve('billing', 'vhost_a', 'q1');
    }
}
