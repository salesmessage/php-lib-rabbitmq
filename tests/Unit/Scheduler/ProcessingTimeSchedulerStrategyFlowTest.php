<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Scheduler;

use Salesmessage\LibRabbitMQ\Services\Scheduler\ProcessingTimeSchedulerStrategy;
use Salesmessage\LibRabbitMQ\Tests\Support\RedisBackedTestCase;

/**
 * End-to-end flow over real InternalStorageManager: real keys, real bucket
 * math, real SORT-based ordering. Runs on FakeRedisConnection by default and
 * on a live Redis with USE_REAL_REDIS=1 (see RedisBackedTestCase).
 */
class ProcessingTimeSchedulerStrategyFlowTest extends RedisBackedTestCase
{
    private ProcessingTimeSchedulerStrategy $scheduler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->scheduler = new ProcessingTimeSchedulerStrategy($this->storage, [
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
        $this->assertNull($this->windowCost('v3', 'g')); // refunded to 0 -> field cleared
        $this->assertSame('8000', $this->windowCost('v1', 'g')); // 3000 + 5000 provisional

        // v1's second job took 2 seconds: total is exactly 3000 + 2000
        $this->scheduler->record('g', 'v1', 'q1', 2000);
        $this->assertSame('5000', $this->windowCost('v1', 'g'));
        $this->assertSame(['v3', 'v2', 'v1'], $this->scheduler->getOrderedVhosts('g'));
    }

    public function testAccrualReflectsLongJobBeforeRecordAndReconcilesExactly(): void
    {
        // a worker picks v1: provisional 5000 applies immediately
        $this->scheduler->reserve('g', 'v1', 'q1');
        $this->assertSame('5000', $this->windowCost('v1', 'g'));

        // the job is long-running: accrual raises the cost above the provisional
        // while it is still executing, so other workers see v1 as busy
        $this->scheduler->accrue('g', 'v1', 30000);
        $this->assertSame('30000', $this->windowCost('v1', 'g'));

        $this->scheduler->accrue('g', 'v1', 90000);
        $this->assertSame('90000', $this->windowCost('v1', 'g'));

        // the job finishes at 92s: record reconciles to the exact real time
        $this->scheduler->record('g', 'v1', 'q1', 92000);
        $this->assertSame('92000', $this->windowCost('v1', 'g'));
    }

    public function testAccrualNeverDecreasesTheChargeAndRecordCanCorrectDownward(): void
    {
        $this->scheduler->reserve('g', 'v1', 'q1');

        $this->scheduler->accrue('g', 'v1', 40000);
        $this->assertSame('40000', $this->windowCost('v1', 'g'));

        // a smaller elapsed never reduces the in-flight charge
        $this->scheduler->accrue('g', 'v1', 20000);
        $this->assertSame('40000', $this->windowCost('v1', 'g'));

        // the measured duration came in under the last accrual: record still
        // reconciles the total to the exact real time (a downward correction)
        $this->scheduler->record('g', 'v1', 'q1', 39000);
        $this->assertSame('39000', $this->windowCost('v1', 'g'));
    }

    public function testAccrualWithoutAnActiveReservationIsANoop(): void
    {
        $this->scheduler->accrue('g', 'v1', 30000);

        $this->assertNull($this->windowCost('v1', 'g'));
    }

    public function testEqualCostVhostsAreAllPresentAndPrecedeCostlierOnes(): void
    {
        $this->scheduler->record('g', 'v2', 'q1', 9000);

        $ordered = $this->scheduler->getOrderedVhosts('g');

        $this->assertSame('v2', end($ordered));
        $this->assertEqualsCanonicalizing(['v1', 'v3'], array_slice($ordered, 0, 2));
    }

    public function testQueueOrderingWithinVhostIsByProcessingTime(): void
    {
        $this->indexQueue('v1', 'q1', ['g']);
        $this->indexQueue('v1', 'q2', ['g']);
        $this->indexQueue('v1', 'q3', ['g']);

        // q1 did the most work, q2 some, q3 none: least-busy queue is ordered first
        $this->scheduler->record('g', 'v1', 'q1', 5000);
        $this->scheduler->record('g', 'v1', 'q2', 2000);

        $ordered = $this->scheduler->getOrderedQueues('g', 'v1');

        $this->assertSame('q3', $ordered[0]);
        $this->assertSame('q1', end($ordered));
        $this->assertSame(['q3', 'q2', 'q1'], $ordered);
    }

    public function testQueueLevelReserveAccrueRecordIsFairAndExact(): void
    {
        $this->indexQueue('v1', 'q1', ['g']);
        $this->indexQueue('v1', 'q2', ['g']);

        // a worker picks q1 for a long job: the provisional applies to the queue too
        $this->scheduler->reserve('g', 'v1', 'q1');
        $this->assertSame('5000', $this->queueWindowCost('v1', 'q1', 'g'));

        // while it runs, accrual raises q1's cost so q2 becomes preferred
        $this->scheduler->accrue('g', 'v1', 60000);
        $this->assertSame('60000', $this->queueWindowCost('v1', 'q1', 'g'));
        $this->assertSame('q2', $this->scheduler->getOrderedQueues('g', 'v1')[0]);

        // the job finishes: the queue cost reconciles to the exact real time,
        // and the vhost carries the same total aggregated across its queues
        $this->scheduler->record('g', 'v1', 'q1', 61000);
        $this->assertSame('61000', $this->queueWindowCost('v1', 'q1', 'g'));
        $this->assertSame('61000', $this->windowCost('v1', 'g'));
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
