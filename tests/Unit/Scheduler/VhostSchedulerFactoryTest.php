<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Scheduler;

use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Scheduler\LastProcessedSchedulerStrategy;
use Salesmessage\LibRabbitMQ\Services\Scheduler\ProcessingTimeSchedulerStrategy;
use Salesmessage\LibRabbitMQ\Services\Scheduler\VhostSchedulerFactory;

class VhostSchedulerFactoryTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function testMakesLastProcessedByDefault(): void
    {
        $scheduler = VhostSchedulerFactory::make(
            Mockery::mock(InternalStorageManager::class),
            'last_processed'
        );

        $this->assertInstanceOf(LastProcessedSchedulerStrategy::class, $scheduler);
    }

    public function testMakesProcessingTimeFromStrategy(): void
    {
        $scheduler = VhostSchedulerFactory::make(
            Mockery::mock(InternalStorageManager::class),
            'processing_time',
            ['window' => 300, 'bucket' => 30, 'reservation_estimate' => 2]
        );

        $this->assertInstanceOf(ProcessingTimeSchedulerStrategy::class, $scheduler);
    }

    public function testUnknownStrategyFallsBackToLastProcessed(): void
    {
        $scheduler = VhostSchedulerFactory::make(
            Mockery::mock(InternalStorageManager::class),
            'bogus'
        );

        $this->assertInstanceOf(LastProcessedSchedulerStrategy::class, $scheduler);
    }
}
