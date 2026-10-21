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

        $vhosts = (new QueueMetricsAggregator)->aggregate(['rabbit@n1' => $node1, 'rabbit@n2' => $node2]);

        $this->assertSame([
            ['name' => 'org_1', 'messages' => 0, 'messages_ready' => 0, 'messages_unacknowledged' => 0],
            ['name' => 'org_2', 'messages' => 8, 'messages_ready' => 6, 'messages_unacknowledged' => 2],
        ], array_map(fn (VhostApiDto $vhost): array => $vhost->toInternalData(), $vhosts));
    }

    public function test_queue_reported_by_two_nodes_is_counted_once_with_the_higher_value(): void
    {
        $node1 = $this->node('rabbit@n1')->setQueueCount('org_1', 'q1', 'messages_ready', 2);
        $node2 = $this->node('rabbit@n2')->setQueueCount('org_1', 'q1', 'messages_ready', 3);

        $vhosts = (new QueueMetricsAggregator)->aggregate(['rabbit@n1' => $node1, 'rabbit@n2' => $node2]);

        $this->assertCount(1, $vhosts);
        $this->assertSame(3, $vhosts[0]->getMessagesReady());
    }

    public function test_vhost_carries_queues_with_messages_and_queues_without_counts(): void
    {
        $node1 = $this->node('rabbit@n1')
            ->setQueueCount('org_1', 'q_busy', 'messages', 3)
            ->setQueueCount('org_1', 'q_busy', 'messages_ready', 1)
            ->setQueueCount('org_1', 'q_busy', 'messages_unacknowledged', 2)
            ->setQueueCount('org_1', 'q_idle', 'messages', 0)
            ->setQueueCount('org_1', 'q_idle', 'messages_ready', 0)
            ->setQueueCount('org_1', 'q_idle', 'messages_unacknowledged', 0)
            ->addQueue('org_1', 'q_busy')
            ->addQueue('org_1', 'q_idle')
            ->addQueue('org_1', 'q_electing');
        $node2 = $this->node('rabbit@n2')
            ->addQueue('org_1', 'q_busy')
            ->addQueue('org_1', 'q_electing');

        $vhosts = (new QueueMetricsAggregator)->aggregate(['rabbit@n1' => $node1, 'rabbit@n2' => $node2]);

        $this->assertCount(1, $vhosts);
        $this->assertSame([
            'q_busy' => ['messages' => 3, 'messages_ready' => 1, 'messages_unacknowledged' => 2],
            'q_electing' => null,
        ], $vhosts[0]->toInterimData()['queues']);
        $this->assertTrue($vhosts[0]->isQueuesComplete());
    }

    public function test_vhost_with_only_queues_without_counts_has_zero_totals(): void
    {
        $vhosts = (new QueueMetricsAggregator)->aggregate([
            'rabbit@n1' => $this->node('rabbit@n1')->addQueue('org_1', 'q_electing'),
        ]);

        $this->assertSame([[
            'name' => 'org_1',
            'messages' => 0,
            'messages_ready' => 0,
            'messages_unacknowledged' => 0,
            'queues' => ['q_electing' => null],
            'queues_complete' => true,
        ]], array_map(fn (VhostApiDto $vhost): array => $vhost->toInterimData(), $vhosts));
    }

    public function test_metrics_from_unexpected_node_throws(): void
    {
        $this->expectException(PrometheusMetricsException::class);
        $this->expectExceptionMessage('Metrics fetched for node rabbit@n1 were reported by node rabbit@other');

        (new QueueMetricsAggregator)->aggregate(['rabbit@n1' => $this->node('rabbit@other')]);
    }

    public function test_node_without_queue_counts_is_valid(): void
    {
        $vhosts = (new QueueMetricsAggregator)->aggregate([
            'rabbit@n1' => $this->node('rabbit@n1')->setQueueCount('org_1', 'q1', 'messages_ready', 2),
            'rabbit@n2' => $this->node('rabbit@n2'),
        ]);

        $this->assertCount(1, $vhosts);
        $this->assertSame(2, $vhosts[0]->getMessagesReady());
    }

    public function test_nodes_without_any_queue_counts_return_no_vhosts(): void
    {
        $vhosts = (new QueueMetricsAggregator)->aggregate([
            'rabbit@n1' => $this->node('rabbit@n1'),
            'rabbit@n2' => $this->node('rabbit@n2'),
        ]);

        $this->assertSame([], $vhosts);
    }

    public function test_node_without_identity_throws(): void
    {
        $this->expectException(PrometheusMetricsException::class);
        $this->expectExceptionMessage('Node rabbit@n2 returned no rabbitmq_identity_info');

        (new QueueMetricsAggregator)->aggregate([
            'rabbit@n1' => $this->node('rabbit@n1')->setQueueCount('org_1', 'q1', 'messages', 0),
            'rabbit@n2' => new NodeQueueMetricsDto,
        ]);
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
