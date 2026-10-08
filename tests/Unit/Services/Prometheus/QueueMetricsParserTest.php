<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services\Prometheus;

use GuzzleHttp\Psr7\Utils;
use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;
use Salesmessage\LibRabbitMQ\Services\Prometheus\QueueMetricsParser;

class QueueMetricsParserTest extends TestCase
{
    public function test_parses_counts_and_identity(): void
    {
        $body = <<<'PROM'
# TYPE rabbitmq_detailed_queue_messages_ready gauge
# HELP rabbitmq_detailed_queue_messages_ready Messages ready to be delivered to consumers
rabbitmq_detailed_queue_messages_ready{vhost="org_1",queue="q1"} 4
rabbitmq_detailed_queue_messages_unacked{vhost="org_1",queue="q1"} 2
rabbitmq_detailed_queue_messages{vhost="org_1",queue="q1"} 6
rabbitmq_detailed_queue_process_reductions_total{vhost="org_1",queue="q1"} 123456
rabbitmq_detailed_queue_info{vhost="org_1",queue="q1",queue_type="rabbit_quorum_queue",membership="leader"} 1
rabbitmq_detailed_queue_info{vhost="org_2",queue="q2",queue_type="rabbit_quorum_queue",membership="follower"} 1
rabbitmq_build_info{rabbitmq_version="4.3.1"} 1
rabbitmq_identity_info{rabbitmq_node="rabbit@10.0.0.1",rabbitmq_cluster="c",rabbitmq_cluster_permanent_id="cid"} 1
telemetry_scrape_duration_seconds_sum{registry="detailed"} 0.5
PROM;

        $metrics = (new QueueMetricsParser)->parse(Utils::streamFor($body));

        $this->assertSame('rabbit@10.0.0.1', $metrics->getNodeName());
        $this->assertSame('cid', $metrics->getClusterId());
        $this->assertSame(
            ['org_1' => ['q1' => ['messages_ready' => 4, 'messages_unacknowledged' => 2, 'messages' => 6]]],
            $metrics->getQueueCounts()
        );
    }

    public function test_unescapes_label_values(): void
    {
        $body = 'rabbitmq_detailed_queue_messages_ready{vhost="org_1",queue="a\\"b\\\\c} d"} 7'."\n";

        $metrics = (new QueueMetricsParser)->parse(Utils::streamFor($body));

        $this->assertSame(['org_1' => ['a"b\\c} d' => ['messages_ready' => 7]]], $metrics->getQueueCounts());
    }

    public function test_reads_lines_split_across_chunks(): void
    {
        $lines = [];
        for ($i = 0; $i < 3000; $i++) {
            $lines[] = sprintf('rabbitmq_detailed_queue_messages_ready{vhost="org_%d",queue="queue_with_a_long_name_%d"} %d', $i, $i, $i);
        }

        $metrics = (new QueueMetricsParser)->parse(Utils::streamFor(implode("\n", $lines)));

        $counts = $metrics->getQueueCounts();
        $this->assertCount(3000, $counts);
        $this->assertSame(2999, $counts['org_2999']['queue_with_a_long_name_2999']['messages_ready']);
    }

    public function test_malformed_sample_throws(): void
    {
        $this->expectException(PrometheusMetricsException::class);

        (new QueueMetricsParser)->parse(Utils::streamFor('rabbitmq_detailed_queue_messages{vhost="v",queue="q"} NaN'));
    }

    public function test_count_without_queue_label_throws(): void
    {
        $this->expectException(PrometheusMetricsException::class);

        (new QueueMetricsParser)->parse(Utils::streamFor('rabbitmq_detailed_queue_messages{vhost="v"} 1'));
    }
}
