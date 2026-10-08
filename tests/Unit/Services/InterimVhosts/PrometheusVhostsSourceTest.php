<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services\InterimVhosts;

use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Psr\Http\Message\RequestInterface;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;
use Salesmessage\LibRabbitMQ\Services\Api\PrometheusClient;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Salesmessage\LibRabbitMQ\Services\InterimVhosts\PrometheusVhostsSource;
use Salesmessage\LibRabbitMQ\Services\Prometheus\QueueMetricsAggregator;
use Salesmessage\LibRabbitMQ\Services\Prometheus\QueueMetricsParser;
use Salesmessage\LibRabbitMQ\Tests\TestCase;

class PrometheusVhostsSourceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    /** @var array<string> */
    private array $requestedUris = [];

    public function test_sums_counts_of_all_nodes(): void
    {
        $source = $this->makeSource(
            [
                ['name' => 'rabbit@10.0.0.1', 'running' => true],
                ['name' => 'rabbit@10.0.0.2', 'running' => true],
            ],
            [
                '10.0.0.1' => new Response(200, [], $this->nodeMetrics('rabbit@10.0.0.1', [
                    ['org_1', 'q1', 3, 1, 'leader'],
                    ['org_2', 'q1', 0, 0, 'follower'],
                ])),
                '10.0.0.2' => new Response(200, [], $this->nodeMetrics('rabbit@10.0.0.2', [
                    ['org_1', 'q1', 0, 0, 'follower'],
                    ['org_2', 'q1', 0, 2, 'leader'],
                ])),
            ]
        );

        $vhosts = $source->getVhosts();

        $this->assertSame([
            ['name' => 'org_1', 'messages' => 4, 'messages_ready' => 3, 'messages_unacknowledged' => 1],
            ['name' => 'org_2', 'messages' => 2, 'messages_ready' => 0, 'messages_unacknowledged' => 2],
        ], array_map(fn (VhostApiDto $vhost): array => $vhost->toInternalData(), $vhosts));

        sort($this->requestedUris);
        $this->assertSame([
            'http://10.0.0.1:15692/metrics/detailed?family=queue_coarse_metrics',
            'http://10.0.0.2:15692/metrics/detailed?family=queue_coarse_metrics',
        ], $this->requestedUris);
    }

    public function test_queue_without_a_leader_adds_nothing_to_its_vhost(): void
    {
        $source = $this->makeSource(
            [
                ['name' => 'rabbit@10.0.0.1', 'running' => true],
                ['name' => 'rabbit@10.0.0.2', 'running' => true],
            ],
            [
                '10.0.0.1' => new Response(200, [], $this->nodeMetrics('rabbit@10.0.0.1', [
                    ['org_1', 'q1', 1, 0, 'leader'],
                    ['org_1', 'q2', 7, 0, 'follower'],
                    ['org_2', 'q1', 0, 0, 'follower'],
                ])),
                '10.0.0.2' => new Response(200, [], $this->nodeMetrics('rabbit@10.0.0.2', [
                    ['org_1', 'q1', 0, 0, 'follower'],
                    ['org_1', 'q2', 7, 0, 'follower'],
                    ['org_2', 'q1', 0, 0, 'follower'],
                ])),
            ]
        );

        $this->assertSame(
            [['name' => 'org_1', 'messages' => 1, 'messages_ready' => 1, 'messages_unacknowledged' => 0]],
            array_map(fn (VhostApiDto $vhost): array => $vhost->toInternalData(), $source->getVhosts())
        );
    }

    public function test_uses_configured_prometheus_port(): void
    {
        $this->app['config']->set('queue.connections.rabbitmq_vhosts.prometheus_port', 15999);

        $source = $this->makeSource(
            [['name' => 'rabbit@10.0.0.1', 'running' => true]],
            ['10.0.0.1' => new Response(200, [], $this->nodeMetrics('rabbit@10.0.0.1', []))]
        );

        $source->getVhosts();

        $this->assertSame(['http://10.0.0.1:15999/metrics/detailed?family=queue_coarse_metrics'], $this->requestedUris);
    }

    public function test_unreachable_node_fails_the_pass(): void
    {
        $source = $this->makeSource(
            [
                ['name' => 'rabbit@10.0.0.1', 'running' => true],
                ['name' => 'rabbit@10.0.0.2', 'running' => true],
            ],
            [
                '10.0.0.1' => new Response(200, [], $this->nodeMetrics('rabbit@10.0.0.1', [])),
                '10.0.0.2' => 'connection refused',
            ]
        );

        $this->expectException(PrometheusMetricsException::class);
        $this->expectExceptionMessage('10.0.0.2: connection refused');

        $source->getVhosts();
    }

    public function test_error_response_fails_the_pass(): void
    {
        $source = $this->makeSource(
            [['name' => 'rabbit@10.0.0.1', 'running' => true]],
            ['10.0.0.1' => new Response(503)]
        );

        $this->expectException(PrometheusMetricsException::class);

        $source->getVhosts();
    }

    public function test_stopped_node_fails_the_pass_before_reading_metrics(): void
    {
        $source = $this->makeSource(
            [
                ['name' => 'rabbit@10.0.0.1', 'running' => true],
                ['name' => 'rabbit@10.0.0.2', 'running' => false],
            ],
            ['10.0.0.1' => new Response(200, [], $this->nodeMetrics('rabbit@10.0.0.1', []))]
        );

        try {
            $source->getVhosts();
            $this->fail('A stopped node must fail the pass');
        } catch (PrometheusMetricsException $exception) {
            $this->assertSame('RabbitMQ nodes are not running: rabbit@10.0.0.2', $exception->getMessage());
        }

        $this->assertSame([], $this->requestedUris);
    }

    public function test_no_nodes_fails_the_pass(): void
    {
        $source = $this->makeSource([], []);

        $this->expectException(PrometheusMetricsException::class);
        $this->expectExceptionMessage('reported no nodes');

        $source->getVhosts();
    }

    /**
     * @param  array  $nodes  /api/nodes response
     * @param  array<string, Response|string>  $responses  per host; a string fails the connection with that message
     */
    private function makeSource(array $nodes, array $responses): PrometheusVhostsSource
    {
        $rabbitApiClient = Mockery::mock(RabbitApiClient::class);
        $rabbitApiClient->shouldReceive('setConnectionConfig')->andReturnSelf();
        $rabbitApiClient->shouldReceive('request')
            ->with('GET', '/api/nodes', ['columns' => 'name,running'])
            ->andReturn($nodes);

        $handler = function (RequestInterface $request) use ($responses) {
            $this->requestedUris[] = (string) $request->getUri();

            $response = $responses[$request->getUri()->getHost()];
            if (is_string($response)) {
                return Create::rejectionFor(new ConnectException($response, $request));
            }

            return Create::promiseFor($response);
        };

        $source = new PrometheusVhostsSource(
            $rabbitApiClient,
            new PrometheusClient(new HttpClient(['handler' => HandlerStack::create($handler)])),
            new QueueMetricsParser,
            new QueueMetricsAggregator
        );

        return $source->setConnection('rabbitmq_vhosts');
    }

    /**
     * @param  array<array{0: string, 1: string, 2: int, 3: int, 4: string}>  $queues  vhost, queue, ready, unacked, membership
     */
    private function nodeMetrics(string $nodeName, array $queues): string
    {
        $lines = [sprintf(
            'rabbitmq_identity_info{rabbitmq_node="%s",rabbitmq_cluster_permanent_id="cluster"} 1',
            $nodeName
        )];

        foreach ($queues as [$vhost, $queue, $ready, $unacked, $membership]) {
            $labels = sprintf('vhost="%s",queue="%s"', $vhost, $queue);
            $lines[] = sprintf('rabbitmq_detailed_queue_info{%s,membership="%s"} 1', $labels, $membership);

            if ($membership === 'leader') {
                $lines[] = sprintf('rabbitmq_detailed_queue_messages_ready{%s} %d', $labels, $ready);
                $lines[] = sprintf('rabbitmq_detailed_queue_messages_unacked{%s} %d', $labels, $unacked);
                $lines[] = sprintf('rabbitmq_detailed_queue_messages{%s} %d', $labels, $ready + $unacked);
            }
        }

        return implode("\n", $lines)."\n";
    }
}
