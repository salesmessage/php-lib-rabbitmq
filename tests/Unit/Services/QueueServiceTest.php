<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services;

use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Psr\Log\NullLogger;
use RuntimeException;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Salesmessage\LibRabbitMQ\Services\QueueService;
use Salesmessage\LibRabbitMQ\Tests\TestCase;

class QueueServiceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function test_collects_queues_of_all_pages(): void
    {
        $service = $this->makeService([
            1 => ['items' => [['name' => 'q1']], 'page_count' => 2],
            2 => ['items' => [['name' => 'q2']], 'page_count' => 2],
        ]);

        $queues = $service->getAllVhostQueues(new VhostApiDto(['name' => 'org_1']));

        $this->assertSame(['q1', 'q2'], $queues->pluck('name')->all());
    }

    public function test_failed_page_discards_the_partial_list(): void
    {
        $service = $this->makeService([
            1 => ['items' => [['name' => 'q1']], 'page_count' => 2],
            2 => new RuntimeException('timeout'),
        ]);

        $this->assertNull($service->getAllVhostQueues(new VhostApiDto(['name' => 'org_1'])));
    }

    /**
     * @param  array<int, array|\Throwable>  $pages
     */
    private function makeService(array $pages): QueueService
    {
        $client = Mockery::mock(RabbitApiClient::class);
        $client->shouldReceive('setConnectionConfig')->andReturnSelf();
        $client->shouldReceive('request')->andReturnUsing(function (string $method, string $uri, array $query) use ($pages) {
            $page = $pages[$query['page']];
            if ($page instanceof \Throwable) {
                throw $page;
            }

            return $page;
        });

        return new QueueService($client, new NullLogger);
    }
}
