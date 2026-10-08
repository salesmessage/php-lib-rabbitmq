<?php

namespace Salesmessage\LibRabbitMQ\Services\Prometheus;

use Salesmessage\LibRabbitMQ\Dto\NodeQueueMetricsDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;

class QueueMetricsAggregator
{
    private const COUNT_FIELDS = ['messages', 'messages_ready', 'messages_unacknowledged'];

    private const MISSING_QUEUES_EXAMPLES = 5;

    /**
     * @param  array<string, NodeQueueMetricsDto>  $nodesMetrics  keyed by the expected node name
     * @return array<VhostApiDto> sorted by vhost name
     *
     * @throws PrometheusMetricsException
     */
    public function aggregate(array $nodesMetrics): array
    {
        $this->assertSameCluster($nodesMetrics);

        $queueCounts = $this->mergeQueueCounts($nodesMetrics);

        $this->assertEveryQueueCounted($nodesMetrics, $queueCounts);

        ksort($queueCounts, SORT_STRING);

        $vhosts = [];
        foreach ($queueCounts as $vhostName => $queues) {
            $totals = array_fill_keys(self::COUNT_FIELDS, 0);
            foreach ($queues as $counts) {
                foreach (self::COUNT_FIELDS as $field) {
                    $totals[$field] += $counts[$field] ?? 0;
                }
            }

            $vhosts[] = new VhostApiDto(['name' => (string) $vhostName] + $totals);
        }

        return $vhosts;
    }

    /**
     * @param  array<string, NodeQueueMetricsDto>  $nodesMetrics
     *
     * @throws PrometheusMetricsException
     */
    private function assertSameCluster(array $nodesMetrics): void
    {
        $clusterIds = [];
        foreach ($nodesMetrics as $expectedNodeName => $nodeMetrics) {
            $reportedNodeName = $nodeMetrics->getNodeName();
            if (($reportedNodeName !== null) && ($reportedNodeName !== (string) $expectedNodeName)) {
                throw new PrometheusMetricsException(sprintf(
                    'Metrics fetched for node %s were reported by node %s',
                    $expectedNodeName,
                    $reportedNodeName
                ));
            }

            if ($nodeMetrics->getClusterId() !== null) {
                $clusterIds[$nodeMetrics->getClusterId()] = true;
            }
        }

        if (count($clusterIds) > 1) {
            throw new PrometheusMetricsException(sprintf(
                'Nodes belong to different clusters: %s',
                implode(', ', array_keys($clusterIds))
            ));
        }
    }

    /**
     * @param  array<string, NodeQueueMetricsDto>  $nodesMetrics
     * @return array<string, array<string, array<string, int>>>
     */
    private function mergeQueueCounts(array $nodesMetrics): array
    {
        $merged = [];
        foreach ($nodesMetrics as $nodeMetrics) {
            foreach ($nodeMetrics->getQueueCounts() as $vhostName => $queues) {
                foreach ($queues as $queueName => $counts) {
                    // around a leader change a queue can be reported by two nodes for a moment;
                    // taking the max can only flag an idle vhost as busy (one extra queue fetch),
                    // whereas summing or picking one could hide messages and de-index a busy vhost
                    foreach ($counts as $field => $count) {
                        $merged[$vhostName][$queueName][$field] = max($merged[$vhostName][$queueName][$field] ?? 0, $count);
                    }
                }
            }
        }

        return $merged;
    }

    /**
     * @param  array<string, NodeQueueMetricsDto>  $nodesMetrics
     * @param  array<string, array<string, array<string, int>>>  $queueCounts
     *
     * @throws PrometheusMetricsException
     */
    private function assertEveryQueueCounted(array $nodesMetrics, array $queueCounts): void
    {
        $missing = [];
        foreach ($nodesMetrics as $nodeMetrics) {
            foreach ($nodeMetrics->getQueueMembers() as $vhostName => $queues) {
                foreach ($queues as $queueName => $isMember) {
                    if (! isset($queueCounts[$vhostName][$queueName])) {
                        $missing[$vhostName.'/'.$queueName] = true;
                    }
                }
            }
        }

        if (empty($missing)) {
            return;
        }

        throw new PrometheusMetricsException(sprintf(
            '%d queue(s) have no message counts from any node, e.g. %s',
            count($missing),
            implode(', ', array_slice(array_keys($missing), 0, self::MISSING_QUEUES_EXAMPLES))
        ));
    }
}
