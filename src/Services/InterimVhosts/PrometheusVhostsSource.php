<?php

namespace Salesmessage\LibRabbitMQ\Services\InterimVhosts;

use Salesmessage\LibRabbitMQ\Dto\InterimVhostsDto;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;
use Salesmessage\LibRabbitMQ\Services\Api\PrometheusClient;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Salesmessage\LibRabbitMQ\Services\Prometheus\QueueMetricsAggregator;
use Salesmessage\LibRabbitMQ\Services\Prometheus\QueueMetricsParser;

class PrometheusVhostsSource implements InterimVhostsSourceInterface
{
    private const DEFAULT_PORT = 15692;

    private const DEFAULT_TIMEOUT = 10;

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
     * A stopped node is left out: its quorum queues elect a leader on a running node and its classic
     * queues cannot be consumed. Its vhosts may be missing from the counts, so it is reported as skipped.
     *
     * @throws PrometheusMetricsException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function getVhosts(): InterimVhostsDto
    {
        $runningByNode = $this->getNodes();
        $stoppedNodeNames = array_keys(array_filter($runningByNode, static fn (bool $isRunning): bool => ! $isRunning));

        $hostsByNode = [];
        foreach (array_keys(array_filter($runningByNode)) as $nodeName) {
            $hostsByNode[$nodeName] = $this->getHost($nodeName);
        }

        if (empty($hostsByNode)) {
            throw new PrometheusMetricsException(sprintf(
                'RabbitMQ nodes are not running: %s',
                implode(', ', $stoppedNodeNames)
            ));
        }

        $bodies = $this->prometheusClient->fetchDetailedFamily(
            array_values($hostsByNode),
            $this->getScheme(),
            $this->getPort(),
            self::METRICS_FAMILY,
            $this->getTimeout()
        );

        $nodesMetrics = [];
        foreach ($hostsByNode as $nodeName => $host) {
            $nodesMetrics[$nodeName] = $this->parser->parse($bodies[$host]);
        }

        return new InterimVhostsDto($this->aggregator->aggregate($nodesMetrics), $stoppedNodeNames);
    }

    /**
     * @return array<string, bool> node name => running
     *
     * @throws PrometheusMetricsException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    private function getNodes(): array
    {
        $nodes = $this->rabbitApiClient->request('GET', '/api/nodes', [
            'columns' => 'name,running',
        ]);

        $runningByNode = [];
        foreach ($nodes as $node) {
            $runningByNode[(string) ($node['name'] ?? '')] = true === ($node['running'] ?? false);
        }

        if (empty($runningByNode)) {
            throw new PrometheusMetricsException('RabbitMQ management API reported no nodes');
        }

        return $runningByNode;
    }

    /**
     * @throws PrometheusMetricsException
     */
    private function getHost(string $nodeName): string
    {
        $atPosition = strpos($nodeName, '@');
        $host = ($atPosition === false) ? '' : substr($nodeName, $atPosition + 1);
        if ($host === '') {
            throw new PrometheusMetricsException(sprintf('Unexpected RabbitMQ node name "%s"', $nodeName));
        }

        return $host;
    }

    private function getScheme(): string
    {
        return filter_var($this->connectionConfig['prometheus_secure'] ?? false, FILTER_VALIDATE_BOOLEAN) ? 'https' : 'http';
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
