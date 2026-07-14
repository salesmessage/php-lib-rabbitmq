<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Scheduler;

use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Services\Scheduler\LastProcessingBasedScheduler;
use Salesmessage\LibRabbitMQ\Tests\Support\RedisBackedTestCase;

class LastProcessingBasedSchedulerFlowTest extends RedisBackedTestCase
{
    private LastProcessingBasedScheduler $scheduler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->scheduler = new LastProcessingBasedScheduler($this->storage);

        $this->indexVhost('v1', ['g']);
        $this->indexVhost('v2', ['g']);
    }

    private function setVhostLastProcessedAt(string $vhost, int $timestamp): void
    {
        $vhostDto = new VhostApiDto(['name' => $vhost]);
        $vhostDto->setGroupName('g')->setLastProcessedAt($timestamp);
        $this->storage->updateVhostLastProcessedAt($vhostDto);
    }

    public function testOrdersByLeastRecentlyProcessed(): void
    {
        $this->setVhostLastProcessedAt('v1', time() - 100);
        $this->setVhostLastProcessedAt('v2', time() - 200);

        $this->assertSame(['v2', 'v1'], $this->scheduler->getOrderedVhosts('g'));
    }

    public function testReserveMovesVhostToTheEnd(): void
    {
        $this->setVhostLastProcessedAt('v1', time() - 100);
        $this->setVhostLastProcessedAt('v2', time() - 200);

        $this->scheduler->reserve('g', 'v2', 'q1');

        $this->assertSame(['v1', 'v2'], $this->scheduler->getOrderedVhosts('g'));
    }

    public function testRecordMovesVhostToTheEndAndWritesNoWindowCost(): void
    {
        $this->setVhostLastProcessedAt('v1', time() - 100);
        $this->setVhostLastProcessedAt('v2', time() - 200);

        $this->scheduler->record('g', 'v2', 'q1', 12500);

        $this->assertSame(['v1', 'v2'], $this->scheduler->getOrderedVhosts('g'));
        $this->assertNull($this->windowCost('v2', 'g'));
        $this->assertSame(0, (int) $this->redis->exists($this->bucketsKey('g', 'v2')));
    }
}
