<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services\Prometheus;

use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Dto\NodeQueueMetricsDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;
use Salesmessage\LibRabbitMQ\Services\Prometheus\QueueMetricsAggregator;

class QueueMetricsAggregatorTest extends TestCase
{
    public function test_sums_leader_counts_per_vhost_sorted_by_name(): void
    {
        $node1 = $this->node('rabbit@n1')
            ->setQueueCount('org_2', 'q1', 'messages', 3)
            ->setQueueCount('org_2', 'q1', 'messages_ready', 1)
            ->setQueueCount('org_2', 'q1', 'messages_unacknowledged', 2)
            ->setQueueCount('org_1', 'q1', 'messages', 0);
        $node2 = $this->node('rabbit@n2')
            ->setQueueCount('org_2', 'q2', 'messages', 5)
            ->setQueueCount('org_2', 'q2', 'messages_ready', 5)
            ->setQueueCount('org_2', 'q2', 'messages_unacknowledged', 0);

        $vhosts = (new QueueMetricsAggregator)->aggregate(['rabbit@n1' => $node1, 'rabbit@n2' => $node2])->getVhosts();

        $this->assertSame([
            ['name' => 'org_1', 'messages' => 0, 'messages_ready' => 0, 'messages_unacknowledged' => 0],
            ['name' => 'org_2', 'messages' => 8, 'messages_ready' => 6, 'messages_unacknowledged' => 2],
        ], array_map(fn (VhostApiDto $vhost): array => $vhost->toInternalData(), $vhosts));
    }

    public function test_queue_reported_by_two_nodes_is_counted_once_with_the_higher_value(): void
    {
        $node1 = $this->node('rabbit@n1')->setQueueCount('org_1', 'q1', 'messages_ready', 2);
        $node2 = $this->node('rabbit@n2')->setQueueCount('org_1', 'q1', 'messages_ready', 3);

        $vhosts = (new QueueMetricsAggregator)->aggregate(['rabbit@n1' => $node1, 'rabbit@n2' => $node2])->getVhosts();

        $this->assertCount(1, $vhosts);
        $this->assertSame(3, $vhosts[0]->getMessagesReady());
    }

    public function test_vhosts_with_queues_listed_without_counts_are_left_out_and_reported(): void
    {
        $node1 = $this->node('rabbit@n1')
            ->setQueueCount('org_1', 'q1', 'messages_ready', 0)
            ->addQueueMember('org_1', 'q1')
            ->addQueueMember('org_1', 'q2')
            ->setQueueCount('org_2', 'q1', 'messages_ready', 4)
            ->addQueueMember('org_2', 'q1');
        $node2 = $this->node('rabbit@n2')
            ->addQueueMember('org_1', 'q1')
            ->addQueueMember('org_1', 'q2')
            ->addQueueMember('org_2', 'q1')
            ->addQueueMember('org_3', 'q1');

        $result = (new QueueMetricsAggregator)->aggregate(['rabbit@n1' => $node1, 'rabbit@n2' => $node2]);

        $this->assertSame(['org_2'], array_map(fn (VhostApiDto $vhost): string => $vhost->getName(), $result->getVhosts()));
        $this->assertSame(['org_1' => ['q2'], 'org_3' => ['q1']], $result->getUncountedQueues());
    }

    public function test_metrics_from_unexpected_node_throws(): void
    {
        $this->expectException(PrometheusMetricsException::class);
        $this->expectExceptionMessage('Metrics fetched for node rabbit@n1 were reported by node rabbit@other');

        (new QueueMetricsAggregator)->aggregate(['rabbit@n1' => $this->node('rabbit@other')]);
    }

    public function test_nodes_of_different_clusters_throw(): void
    {
        $this->expectException(PrometheusMetricsException::class);

        (new QueueMetricsAggregator)->aggregate([
            'rabbit@n1' => $this->node('rabbit@n1', 'cluster-a'),
            'rabbit@n2' => $this->node('rabbit@n2', 'cluster-b'),
        ]);
    }

    private function node(string $nodeName, string $clusterId = 'cluster'): NodeQueueMetricsDto
    {
        return (new NodeQueueMetricsDto)->setIdentity($nodeName, $clusterId);
    }
}
