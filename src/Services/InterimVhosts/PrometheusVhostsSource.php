<?php

namespace Salesmessage\LibRabbitMQ\Services\InterimVhosts;

use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;
use Salesmessage\LibRabbitMQ\Services\Api\PrometheusClient;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Salesmessage\LibRabbitMQ\Services\Prometheus\QueueMetricsAggregator;
use Salesmessage\LibRabbitMQ\Services\Prometheus\QueueMetricsParser;

class PrometheusVhostsSource implements InterimVhostsSourceInterface
{
    private const DEFAULT_PORT = 15692;

    private const DEFAULT_TIMEOUT = 30;

    private const METRICS_FAMILY = 'queue_coarse_metrics';

    private array $connectionConfig = [];

    public function __construct(
        private RabbitApiClient $rabbitApiClient,
        private PrometheusClient $prometheusClient,
        private QueueMetricsParser $parser,
        private QueueMetricsAggregator $aggregator
    ) {}

    /**
     * @return $this
     */
    public function setConnection(string $connectionName): self
    {
        $this->connectionConfig = (array) config('queue.connections.'.$connectionName, []);
        $this->rabbitApiClient->setConnectionConfig($this->connectionConfig);

        return $this;
    }

    /**
     * @return array<VhostApiDto>
     *
     * @throws PrometheusMetricsException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function getVhosts(): array
    {
        $hostsByNode = $this->getNodeHosts();

        $bodies = $this->prometheusClient->fetchDetailedFamily(
            array_values($hostsByNode),
            $this->getPort(),
            self::METRICS_FAMILY,
            $this->getTimeout()
        );

        $nodesMetrics = [];
        foreach ($hostsByNode as $nodeName => $host) {
            $nodesMetrics[$nodeName] = $this->parser->parse($bodies[$host]);
        }

        return $this->aggregator->aggregate($nodesMetrics);
    }

    /**
     * A stopped node fails the pass like an unreachable one, so the totals never miss the queues it held.
     *
     * @return array<string, string> node name => host
     *
     * @throws PrometheusMetricsException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    private function getNodeHosts(): array
    {
        $nodes = $this->rabbitApiClient->request('GET', '/api/nodes', [
            'columns' => 'name,running',
        ]);

        $hostsByNode = [];
        $stoppedNodeNames = [];
        foreach ($nodes as $node) {
            $nodeName = (string) ($node['name'] ?? '');
            if (true !== ($node['running'] ?? false)) {
                $stoppedNodeNames[] = $nodeName;

                continue;
            }

            $atPosition = strpos($nodeName, '@');
            $host = ($atPosition === false) ? '' : substr($nodeName, $atPosition + 1);
            if ($host === '') {
                throw new PrometheusMetricsException(sprintf('Unexpected RabbitMQ node name "%s"', $nodeName));
            }

            $hostsByNode[$nodeName] = $host;
        }

        if (! empty($stoppedNodeNames)) {
            throw new PrometheusMetricsException(sprintf(
                'RabbitMQ nodes are not running: %s',
                implode(', ', $stoppedNodeNames)
            ));
        }

        if (empty($hostsByNode)) {
            throw new PrometheusMetricsException('RabbitMQ management API reported no nodes');
        }

        return $hostsByNode;
    }

    private function getPort(): int
    {
        return (int) ($this->connectionConfig['prometheus_port'] ?? self::DEFAULT_PORT);
    }

    private function getTimeout(): float
    {
        $timeout = (float) ($this->connectionConfig['prometheus_timeout'] ?? self::DEFAULT_TIMEOUT);

        return $timeout > 0 ? $timeout : self::DEFAULT_TIMEOUT;
    }
}
