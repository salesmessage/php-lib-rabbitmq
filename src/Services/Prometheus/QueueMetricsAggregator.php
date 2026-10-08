<?php

namespace Salesmessage\LibRabbitMQ\Services\Prometheus;

use Salesmessage\LibRabbitMQ\Dto\InterimVhostsDto;
use Salesmessage\LibRabbitMQ\Dto\NodeQueueMetricsDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;

class QueueMetricsAggregator
{
    private const COUNT_FIELDS = ['messages', 'messages_ready', 'messages_unacknowledged'];

    /**
     * @param  array<string, NodeQueueMetricsDto>  $nodesMetrics  keyed by the expected node name
     *
     * @throws PrometheusMetricsException
     */
    public function aggregate(array $nodesMetrics): InterimVhostsDto
    {
        $this->assertSameCluster($nodesMetrics);

        $queueCounts = $this->mergeQueueCounts($nodesMetrics);
        $uncountedQueues = $this->findUncountedQueues($nodesMetrics, $queueCounts);

        ksort($queueCounts, SORT_STRING);

        $vhosts = [];
        // a partial total could flag a busy vhost as idle, so such vhosts are left out
        foreach (array_diff_key($queueCounts, $uncountedQueues) as $vhostName => $queues) {
            $totals = array_fill_keys(self::COUNT_FIELDS, 0);
            foreach ($queues as $counts) {
                foreach (self::COUNT_FIELDS as $field) {
                    $totals[$field] += $counts[$field] ?? 0;
                }
            }

            $vhosts[] = new VhostApiDto(['name' => (string) $vhostName] + $totals);
        }

        return new InterimVhostsDto($vhosts, $uncountedQueues);
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
     * @return array<string, array<string>> vhost => queues
     */
    private function findUncountedQueues(array $nodesMetrics, array $queueCounts): array
    {
        $uncounted = [];
        foreach ($nodesMetrics as $nodeMetrics) {
            foreach ($nodeMetrics->getQueueMembers() as $vhostName => $queues) {
                foreach ($queues as $queueName => $isMember) {
                    if (! isset($queueCounts[$vhostName][$queueName])) {
                        $uncounted[$vhostName][$queueName] = true;
                    }
                }
            }
        }

        return array_map('array_keys', $uncounted);
    }
}
