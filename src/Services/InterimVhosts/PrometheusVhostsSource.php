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
        $hostsByNode = $this->getRunningNodeHosts();

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
     * A stopped node leads no queues, so it is skipped rather than failing the
     * pass; the aggregator still refuses the result if any queue lost its counts.
     *
     * @return array<string, string> node name => host
     *
     * @throws PrometheusMetricsException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    private function getRunningNodeHosts(): array
    {
        $nodes = $this->rabbitApiClient->request('GET', '/api/nodes', [
            'columns' => 'name,running',
        ]);

        $hostsByNode = [];
        foreach ($nodes as $node) {
            if (true !== ($node['running'] ?? false)) {
                continue;
            }

            $nodeName = (string) ($node['name'] ?? '');
            $atPosition = strpos($nodeName, '@');
            $host = ($atPosition === false) ? '' : substr($nodeName, $atPosition + 1);
            if ($host === '') {
                throw new PrometheusMetricsException(sprintf('Unexpected RabbitMQ node name "%s"', $nodeName));
            }

            $hostsByNode[$nodeName] = $host;
        }

        if (empty($hostsByNode)) {
            throw new PrometheusMetricsException('RabbitMQ management API reported no running nodes');
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
