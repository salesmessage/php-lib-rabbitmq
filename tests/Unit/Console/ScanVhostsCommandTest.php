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
