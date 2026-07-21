<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Scheduler;

use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Scheduler\LastProcessingBasedScheduler;
use Salesmessage\LibRabbitMQ\Services\Scheduler\TimeSpentBasedScheduler;
use Salesmessage\LibRabbitMQ\Services\Scheduler\VhostSchedulerFactory;

class VhostSchedulerFactoryTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function testMakesLastProcessingBasedByDefault(): void
    {
        $scheduler = VhostSchedulerFactory::make(
            Mockery::mock(InternalStorageManager::class),
            'last_processing_based'
        );

        $this->assertInstanceOf(LastProcessingBasedScheduler::class, $scheduler);
    }

    public function testMakesTimeSpentBasedFromType(): void
    {
        $scheduler = VhostSchedulerFactory::make(
            Mockery::mock(InternalStorageManager::class),
            'time_spent_based',
            ['window' => 300, 'bucket' => 30, 'reservation_estimate' => 2]
        );

        $this->assertInstanceOf(TimeSpentBasedScheduler::class, $scheduler);
    }

    public function testUnknownTypeFallsBackToLastProcessingBased(): void
    {
        $scheduler = VhostSchedulerFactory::make(
            Mockery::mock(InternalStorageManager::class),
            'bogus'
        );

        $this->assertInstanceOf(LastProcessingBasedScheduler::class, $scheduler);
    }
}
