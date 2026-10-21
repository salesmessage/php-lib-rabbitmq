<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Console;

use Illuminate\Support\Collection;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Salesmessage\LibRabbitMQ\Services\QueueService;
use Salesmessage\LibRabbitMQ\Tests\Support\RedisBackedTestCase;

class ScanVhostsCommandTest extends RedisBackedTestCase
{
    use MockeryPHPUnitIntegration;

    private const BUSY_COUNTS = ['messages' => 2, 'messages_ready' => 2, 'messages_unacknowledged' => 0];

    public function test_failed_queue_fetch_keeps_indexed_queues(): void
    {
        $this->givenBusyInterimVhostWithIndexedQueue('org_1', 'q_old');
        $this->bindQueueService(null);

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['q_old'], $this->storage->getVhostQueues('org_1'));
        $this->assertSame(['org_1'], $this->storage->getVhosts());
    }

    public function test_fetched_queues_replace_indexed_queues(): void
    {
        $this->givenBusyInterimVhostWithIndexedQueue('org_1', 'q_old');
        $this->bindQueueService(new Collection([
            ['name' => 'q_new', 'vhost' => 'org_1', 'messages' => 1, 'messages_ready' => 1],
        ]));

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['q_new'], $this->storage->getVhostQueues('org_1'));
    }

    public function test_queues_read_with_vhost_replace_indexed_queues_without_api_call(): void
    {
        $this->givenIndexedVhostWithQueues('org_1', ['q_old']);
        $this->givenInterimVhostWithQueues('org_1', ['q_busy' => self::BUSY_COUNTS]);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['q_busy'], $this->storage->getVhostQueues('org_1'));
        $this->assertSame('2', $this->redis->hget('rabbitmq_queue|org_1|q_busy', 'messages_ready'));
        $this->assertSame(['org_1'], $this->storage->getVhosts());
        $this->assertSame('2', $this->redis->hget('rabbitmq_vhost|org_1', 'messages_ready'));
    }

    public function test_queue_without_counts_stays_indexed(): void
    {
        $this->givenIndexedVhostWithQueues('org_1', ['q_electing']);
        $this->givenInterimVhostWithQueues('org_1', ['q_busy' => self::BUSY_COUNTS, 'q_electing' => null]);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['q_busy', 'q_electing'], $this->storage->getVhostQueues('org_1'));
    }

    public function test_incomplete_read_keeps_indexed_queues_missing_from_it(): void
    {
        $this->givenIndexedVhostWithQueues('org_1', ['q_on_stopped_node']);
        $this->givenInterimVhostWithQueues('org_1', ['q_busy' => self::BUSY_COUNTS], false);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['q_busy', 'q_on_stopped_node'], $this->storage->getVhostQueues('org_1'));
    }

    public function test_vhost_with_only_queues_without_counts_stays_indexed(): void
    {
        $this->givenIndexedVhostWithQueues('org_1', ['q_electing']);
        $this->givenInterimVhostWithQueues('org_1', ['q_electing' => null]);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['org_1'], $this->storage->getVhosts());
        $this->assertSame(['q_electing'], $this->storage->getVhostQueues('org_1'));
    }

    public function test_idle_vhost_with_incomplete_read_stays_indexed(): void
    {
        $this->givenIndexedVhostWithQueues('org_1', ['q_on_stopped_node']);
        $this->givenInterimVhostWithQueues('org_1', [], false);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['org_1'], $this->storage->getVhosts());
        $this->assertSame(['q_on_stopped_node'], $this->storage->getVhostQueues('org_1'));
    }

    public function test_idle_vhost_is_removed_with_its_queues(): void
    {
        $this->givenIndexedVhostWithQueues('org_1', ['q_old']);
        $this->givenInterimVhostWithQueues('org_1', []);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame([], $this->storage->getVhosts());
        $this->assertSame([], $this->storage->getVhostQueues('org_1'));
    }

    private function givenIndexedVhostWithQueues(string $vhost, array $queues): void
    {
        $this->indexVhost($vhost, []);
        foreach ($queues as $queue) {
            $this->indexQueue($vhost, $queue, []);
        }
    }

    /**
     * @param  array<string, array|null>  $queues  queue => counts, null for a queue without counts
     */
    private function givenInterimVhostWithQueues(string $vhost, array $queues, bool $isComplete = true): void
    {
        $messagesReady = array_sum(array_map(fn (?array $counts): int => $counts['messages_ready'] ?? 0, $queues));

        $this->redis->hset('rabbitmq_interim_vhosts', $vhost, json_encode([
            'name' => $vhost,
            'messages' => $messagesReady,
            'messages_ready' => $messagesReady,
            'messages_unacknowledged' => 0,
            'queues' => $queues,
            'queues_complete' => $isComplete,
        ]));
    }

    private function bindQueueServiceNeverCalled(): void
    {
        $queueService = Mockery::mock(QueueService::class);
        $queueService->shouldReceive('setConnection')->andReturnSelf();
        $queueService->shouldNotReceive('getAllVhostQueues');

        $this->app->instance(QueueService::class, $queueService);
    }

    private function givenBusyInterimVhostWithIndexedQueue(string $vhost, string $queue): void
    {
        $this->redis->hset('rabbitmq_interim_vhosts', $vhost, json_encode([
            'name' => $vhost,
            'messages' => 1,
            'messages_ready' => 1,
            'messages_unacknowledged' => 0,
        ]));

        $this->indexVhost($vhost, []);
        $this->indexQueue($vhost, $queue, []);
    }

    private function bindQueueService(?Collection $queues): void
    {
        $queueService = Mockery::mock(QueueService::class);
        $queueService->shouldReceive('setConnection')->andReturnSelf();
        $queueService->shouldReceive('getAllVhostQueues')->once()->andReturn($queues);

        $this->app->instance(QueueService::class, $queueService);
    }
}
