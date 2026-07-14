<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Scheduler;

use Salesmessage\LibRabbitMQ\Services\Scheduler\LastProcessingBasedScheduler;
use Salesmessage\LibRabbitMQ\Services\Scheduler\TimeSpentBasedScheduler;
use Salesmessage\LibRabbitMQ\Services\Scheduler\VhostSchedulerInterface;
use Salesmessage\LibRabbitMQ\Tests\Support\RedisBackedTestCase;

class SchedulerBindingTest extends RedisBackedTestCase
{
    public function testDefaultsToLastProcessingBasedScheduler(): void
    {
        $this->assertInstanceOf(
            LastProcessingBasedScheduler::class,
            $this->app->make(VhostSchedulerInterface::class)
        );
    }

    public function testBindsTimeSpentBasedSchedulerFromConfig(): void
    {
        $this->app['config']->set('queue.drivers.rabbitmq_vhosts.scheduler.type', 'time_spent_based');

        $this->assertInstanceOf(
            TimeSpentBasedScheduler::class,
            $this->app->make(VhostSchedulerInterface::class)
        );
    }

    public function testUnknownTypeFallsBackToDefault(): void
    {
        $this->app['config']->set('queue.drivers.rabbitmq_vhosts.scheduler.type', 'bogus');

        $this->assertInstanceOf(
            LastProcessingBasedScheduler::class,
            $this->app->make(VhostSchedulerInterface::class)
        );
    }
}
