<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Console;

use Illuminate\Support\Collection;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
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

    public function test_queue_indexed_by_publish_after_counts_were_read_stays_indexed(): void
    {
        $this->givenQueueIndexedByPublish('org_1', 'q_new');
        $this->givenInterimVhostWithQueues('org_1', [], actualizedAt: microtime(true) - 30);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['org_1'], $this->storage->getVhosts());
        $this->assertSame(['q_new'], $this->storage->getVhostQueues('org_1'));
    }

    public function test_queue_indexed_by_publish_shortly_before_counts_were_read_stays_indexed(): void
    {
        $this->givenQueueIndexedByPublish('org_1', 'q_new', microtime(true) - 6);
        $this->givenInterimVhostWithQueues('org_1', [], actualizedAt: microtime(true) - 1);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['org_1'], $this->storage->getVhosts());
        $this->assertSame(['q_new'], $this->storage->getVhostQueues('org_1'));
    }

    public function test_queue_indexed_by_publish_long_before_counts_were_read_is_removed(): void
    {
        $this->givenQueueIndexedByPublish('org_1', 'q_new', microtime(true) - 60);
        $this->givenInterimVhostWithQueues('org_1', [], actualizedAt: microtime(true) - 1);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame([], $this->storage->getVhosts());
        $this->assertSame([], $this->storage->getVhostQueues('org_1'));
    }

    public function test_queue_indexed_by_publish_is_judged_by_pass_start_when_counts_have_no_read_time(): void
    {
        $this->givenQueueIndexedByPublish('org_1', 'q_new');
        $this->givenInterimVhostWithQueues('org_1', []);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['q_new'], $this->storage->getVhostQueues('org_1'));

        config()->set('queue.connections.rabbitmq_vhosts.immediate_indexation_grace', 0);

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame([], $this->storage->getVhostQueues('org_1'));
    }

    public function test_busy_queue_indexed_by_publish_is_removed_once_counts_read_after_it_show_it_idle(): void
    {
        $this->givenQueueIndexedByPublish('org_1', 'q_new');
        $this->givenInterimVhostWithQueues('org_1', ['q_new' => self::BUSY_COUNTS], actualizedAt: microtime(true) - 30);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['q_new'], $this->storage->getVhostQueues('org_1'));

        $this->givenActivatedAt('org_1', 'q_new', microtime(true) - 60);
        $this->givenInterimVhostWithQueues('org_1', [], actualizedAt: microtime(true) - 1);

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame([], $this->storage->getVhosts());
        $this->assertSame([], $this->storage->getVhostQueues('org_1'));
    }

    public function test_vhost_indexed_by_publish_and_missing_from_counts_read_before_it_stays_indexed(): void
    {
        $this->givenQueueIndexedByPublish('org_new', 'q_new');
        $this->givenInterimVhostWithQueues('org_1', [], actualizedAt: microtime(true) - 30);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['org_new'], $this->storage->getVhosts());
        $this->assertSame(['q_new'], $this->storage->getVhostQueues('org_new'));
    }

    public function test_vhost_indexed_by_publish_and_missing_from_counts_read_long_after_it_is_removed(): void
    {
        $this->givenQueueIndexedByPublish('org_new', 'q_new', microtime(true) - 60);
        $this->givenInterimVhostWithQueues('org_1', [], actualizedAt: microtime(true) - 1);
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame([], $this->storage->getVhosts());
    }

    public function test_idle_vhost_without_queue_counts_indexed_by_publish_after_counts_were_read_stays_indexed(): void
    {
        $this->givenQueueIndexedByPublish('org_1', 'q_new');
        $this->redis->hset('rabbitmq_interim_vhosts', 'org_1', json_encode([
            'name' => 'org_1',
            'messages' => 0,
            'messages_ready' => 0,
            'messages_unacknowledged' => 0,
            'actualized_at' => microtime(true) - 30,
        ]));
        $this->bindQueueServiceNeverCalled();

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['org_1'], $this->storage->getVhosts());
        $this->assertSame(['q_new'], $this->storage->getVhostQueues('org_1'));
    }

    public function test_fetched_queues_keep_queue_indexed_by_publish_after_counts_were_read(): void
    {
        $this->givenQueueIndexedByPublish('org_1', 'q_new');
        $this->indexQueue('org_1', 'q_old', []);
        $this->redis->hset('rabbitmq_interim_vhosts', 'org_1', json_encode([
            'name' => 'org_1',
            'messages' => 1,
            'messages_ready' => 1,
            'messages_unacknowledged' => 0,
            'actualized_at' => microtime(true) - 30,
        ]));
        $this->bindQueueService(new Collection([
            ['name' => 'q_busy', 'vhost' => 'org_1', 'messages' => 1, 'messages_ready' => 1],
        ]));

        $this->artisan('lib-rabbitmq:scan-vhosts', ['--type' => 'interim', '--sleep' => 0])->assertExitCode(0);

        $this->assertSame(['q_busy', 'q_new'], $this->storage->getVhostQueues('org_1'));
    }

    private function givenQueueIndexedByPublish(string $vhost, string $queue, ?float $activatedAt = null): void
    {
        $this->storage->activateQueue(new QueueApiDto(['name' => $queue, 'vhost' => $vhost]), []);
        $this->storage->activateVhost(new VhostApiDto(['name' => $vhost]), []);

        if (null !== $activatedAt) {
            $this->givenActivatedAt($vhost, $queue, $activatedAt);
        }
    }

    private function givenActivatedAt(string $vhost, string $queue, float $activatedAt): void
    {
        $this->redis->hset('rabbitmq_queue|' . $vhost . '|' . $queue, 'activated_at', $activatedAt);
        $this->redis->hset('rabbitmq_vhost|' . $vhost, 'activated_at', $activatedAt);
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
    private function givenInterimVhostWithQueues(
        string $vhost,
        array $queues,
        bool $isComplete = true,
        ?float $actualizedAt = null
    ): void {
        $messagesReady = array_sum(array_map(fn (?array $counts): int => $counts['messages_ready'] ?? 0, $queues));

        $this->redis->hset('rabbitmq_interim_vhosts', $vhost, json_encode(array_filter([
            'name' => $vhost,
            'messages' => $messagesReady,
            'messages_ready' => $messagesReady,
            'messages_unacknowledged' => 0,
            'queues' => $queues,
            'queues_complete' => $isComplete,
            'actualized_at' => $actualizedAt,
        ], fn ($value): bool => null !== $value)));
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
