<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Scheduler;

use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Services\Scheduler\ProcessingTimeSchedulerOptions;

class ProcessingTimeSchedulerOptionsTest extends TestCase
{
    public function testDefaults(): void
    {
        $options = ProcessingTimeSchedulerOptions::fromConfig([]);

        $this->assertSame(300, $options->getWindow());
        $this->assertSame(30, $options->getBucket());
        $this->assertSame(5000, $options->getReservationEstimateMs());
    }

    public function testWindowIsFlooredToSixtySeconds(): void
    {
        $options = ProcessingTimeSchedulerOptions::fromConfig(['window' => 5]);

        $this->assertSame(60, $options->getWindow());
    }

    public function testBucketIsCappedAtWindowAndFlooredToOneSecond(): void
    {
        $options = ProcessingTimeSchedulerOptions::fromConfig(['window' => 60, 'bucket' => 600]);
        $this->assertSame(60, $options->getBucket());

        $options = ProcessingTimeSchedulerOptions::fromConfig(['bucket' => 0]);
        $this->assertSame(1, $options->getBucket());
    }

    public function testZeroOrNegativeReservationEstimateDisables(): void
    {
        $this->assertSame(0, ProcessingTimeSchedulerOptions::fromConfig(['reservation_estimate' => 0])->getReservationEstimateMs());
        $this->assertSame(0, ProcessingTimeSchedulerOptions::fromConfig(['reservation_estimate' => -3])->getReservationEstimateMs());
    }

    public function testPositiveReservationEstimateConvertsToMillisecondsWithOneSecondFloor(): void
    {
        $this->assertSame(2500, ProcessingTimeSchedulerOptions::fromConfig(['reservation_estimate' => 2.5])->getReservationEstimateMs());
        $this->assertSame(1000, ProcessingTimeSchedulerOptions::fromConfig(['reservation_estimate' => 0.2])->getReservationEstimateMs());
    }
}
