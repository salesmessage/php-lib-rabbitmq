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
        $this->assertSame(3000, $options->getReservationEstimateMs());
        $this->assertSame(7, $options->getAccrualInterval());
    }

    public function testAccrualIntervalDefaultsIndependentlyOfReservationEstimate(): void
    {
        // a null accrual_interval (config default) falls back to 7, not to reservation_estimate
        $options = ProcessingTimeSchedulerOptions::fromConfig([
            'reservation_estimate' => 10,
            'accrual_interval' => null,
        ]);

        $this->assertSame(7, $options->getAccrualInterval());
    }

    public function testAccrualIntervalIsFlooredToOneSecond(): void
    {
        $this->assertSame(1, ProcessingTimeSchedulerOptions::fromConfig(['accrual_interval' => 0])->getAccrualInterval());
        $this->assertSame(8, ProcessingTimeSchedulerOptions::fromConfig(['accrual_interval' => 8])->getAccrualInterval());
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
