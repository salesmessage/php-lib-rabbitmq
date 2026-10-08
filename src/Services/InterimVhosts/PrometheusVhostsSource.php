<?php

namespace Salesmessage\LibRabbitMQ\Services\InterimVhosts;

use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Dto\InterimVhostsDto;
use Salesmessage\LibRabbitMQ\Dto\NodeQueueMetricsDto;
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
        private QueueMetricsAggregator $aggregator,
        private LoggerInterface $logger
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
     * @throws PrometheusMetricsException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function getVhosts(): InterimVhostsDto
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

        $this->logNodesWithoutQueueInfo($nodesMetrics);

        return $this->aggregator->aggregate($nodesMetrics);
    }

    /**
     * @param  array<string, NodeQueueMetricsDto>  $nodesMetrics
     */
    private function logNodesWithoutQueueInfo(array $nodesMetrics): void
    {
        $nodeNames = [];
        foreach ($nodesMetrics as $nodeName => $nodeMetrics) {
            if (! empty($nodeMetrics->getQueueCounts()) && empty($nodeMetrics->getQueueMembers())) {
                $nodeNames[] = (string) $nodeName;
            }
        }

        if (empty($nodeNames)) {
            return;
        }

        $this->logger->error('Salesmessage.LibRabbitMQ.Services.InterimVhosts.PrometheusVhostsSource.getVhosts.noQueueInfo', [
            'nodes' => $nodeNames,
            'family' => self::METRICS_FAMILY,
            'message' => 'Nodes report queue counts without rabbitmq_detailed_queue_info, queues whose leader was not read go undetected',
        ]);
    }

    /**
     * A stopped node leads no queues, so it is skipped rather than failing the pass;
     * a queue left without a leader is still reported as uncounted by the aggregator.
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
