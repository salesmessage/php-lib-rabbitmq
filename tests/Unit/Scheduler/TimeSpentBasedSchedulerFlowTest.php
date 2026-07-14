<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Scheduler;

use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Services\Scheduler\TimeSpentBasedScheduler;
use Salesmessage\LibRabbitMQ\Tests\Support\RedisBackedTestCase;

/**
 * End-to-end flow over real InternalStorageManager: real keys, real bucket
 * math, real SORT-based ordering. Runs on FakeRedisConnection by default and
 * on a live Redis with USE_REAL_REDIS=1 (see RedisBackedTestCase).
 */
class TimeSpentBasedSchedulerFlowTest extends RedisBackedTestCase
{
    private TimeSpentBasedScheduler $scheduler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->scheduler = new TimeSpentBasedScheduler($this->storage, [
            'window' => 600,
            'bucket' => 60,
            'reservation_estimate' => 5,
        ]);

        foreach (['v1', 'v2', 'v3'] as $vhost) {
            $this->indexVhost($vhost, ['g']);
        }
    }

    public function testColdStartReturnsAllVhosts(): void
    {
        $ordered = $this->scheduler->getOrderedVhosts('g');

        $this->assertEqualsCanonicalizing(['v1', 'v2', 'v3'], $ordered);
    }

    public function testVhostWithRecordedTimeSortsLast(): void
    {
        $this->scheduler->record('g', 'v1', 'q1', 3000);

        $ordered = $this->scheduler->getOrderedVhosts('g');

        $this->assertSame('v1', end($ordered));
        $this->assertEqualsCanonicalizing(['v2', 'v3'], array_slice($ordered, 0, 2));
    }

    public function testFullFlowReserveRecordRefundProducesExactCosts(): void
    {
        // v1 does 3 seconds of real work
        $this->scheduler->record('g', 'v1', 'q1', 3000);

        // a worker picks v2: provisional 5000 applies immediately
        $this->scheduler->reserve('g', 'v2', 'q1');
        $this->assertSame(['v3', 'v1', 'v2'], $this->scheduler->getOrderedVhosts('g'));

        // v2's real work was only 1 second: reconciles to exactly 1000
        $this->scheduler->record('g', 'v2', 'q1', 1000);
        $this->assertSame('1000', $this->windowCost('v2', 'g'));
        $this->assertSame(['v3', 'v2', 'v1'], $this->scheduler->getOrderedVhosts('g'));

        // v3 is picked but its queue is empty; picking v1 refunds v3 in full
        $this->scheduler->reserve('g', 'v3', 'q1');
        $this->scheduler->reserve('g', 'v1', 'q1');
        $this->assertSame('0', $this->windowCost('v3', 'g'));
        $this->assertSame('8000', $this->windowCost('v1', 'g')); // 3000 + 5000 provisional

        // v1's second job took 2 seconds: total is exactly 3000 + 2000
        $this->scheduler->record('g', 'v1', 'q1', 2000);
        $this->assertSame('5000', $this->windowCost('v1', 'g'));
        $this->assertSame(['v3', 'v2', 'v1'], $this->scheduler->getOrderedVhosts('g'));
    }

    public function testEqualCostVhostsAreAllPresentAndPrecedeCostlierOnes(): void
    {
        $this->scheduler->record('g', 'v2', 'q1', 9000);

        $ordered = $this->scheduler->getOrderedVhosts('g');

        $this->assertSame('v2', end($ordered));
        $this->assertEqualsCanonicalizing(['v1', 'v3'], array_slice($ordered, 0, 2));
    }

    public function testQueueOrderingWithinVhostStaysRecencyBased(): void
    {
        $this->indexQueue('v1', 'q1', ['g']);
        $this->indexQueue('v1', 'q2', ['g']);

        $queueDto = new QueueApiDto(['name' => 'q1', 'vhost' => 'v1']);
        $queueDto->setGroupName('g')->setLastProcessedAt(time());
        $this->storage->updateQueueLastProcessedAt($queueDto);

        $this->assertSame(['q2', 'q1'], $this->scheduler->getOrderedQueues('g', 'v1'));
    }

    public function testGroupsAreIsolated(): void
    {
        $this->scheduler->record('g', 'v1', 'q1', 9000);
        $this->scheduler->record('other', 'v2', 'q1', 9000);

        $orderedG = $this->scheduler->getOrderedVhosts('g');
        $orderedOther = $this->scheduler->getOrderedVhosts('other');

        $this->assertSame('v1', end($orderedG));
        $this->assertSame('v2', end($orderedOther));
    }
}
