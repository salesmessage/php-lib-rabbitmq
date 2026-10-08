<?php

namespace Salesmessage\LibRabbitMQ\Services\Prometheus;

use Generator;
use Psr\Http\Message\StreamInterface;
use Salesmessage\LibRabbitMQ\Dto\NodeQueueMetricsDto;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;

class QueueMetricsParser
{
    private const READ_CHUNK_SIZE = 65536;

    private const METRIC_QUEUE_INFO = 'rabbitmq_detailed_queue_info';

    private const METRIC_IDENTITY_INFO = 'rabbitmq_identity_info';

    private const COUNT_FIELDS = [
        'rabbitmq_detailed_queue_messages' => 'messages',
        'rabbitmq_detailed_queue_messages_ready' => 'messages_ready',
        'rabbitmq_detailed_queue_messages_unacked' => 'messages_unacknowledged',
    ];

    private const SAMPLE_PATTERN = '/^[^{]+\{(.*)\}\s+(\S+)/';

    private const LABEL_PATTERN = '/([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\\\]|\\\\.)*)"/';

    private const LABEL_ESCAPES = ['\\\\' => '\\', '\\"' => '"', '\\n' => "\n"];

    /**
     * @throws PrometheusMetricsException
     */
    public function parse(StreamInterface $body): NodeQueueMetricsDto
    {
        $metrics = new NodeQueueMetricsDto;

        foreach ($this->readLines($body) as $line) {
            if ($line === '' || $line[0] === '#') {
                continue;
            }

            $labelsStart = strpos($line, '{');
            if ($labelsStart === false) {
                continue;
            }

            $name = substr($line, 0, $labelsStart);
            $field = self::COUNT_FIELDS[$name] ?? null;
            if (($field === null) && ($name !== self::METRIC_QUEUE_INFO) && ($name !== self::METRIC_IDENTITY_INFO)) {
                continue;
            }

            [$labels, $value] = $this->parseSample($line);

            if ($name === self::METRIC_IDENTITY_INFO) {
                $metrics->setIdentity(
                    $labels['rabbitmq_node'] ?? null,
                    $labels['rabbitmq_cluster_permanent_id'] ?? null
                );

                continue;
            }

            if (! isset($labels['vhost'], $labels['queue'])) {
                throw new PrometheusMetricsException(sprintf('Metric %s has no vhost or queue label: %s', $name, $line));
            }

            if ($field === null) {
                $metrics->addQueueMember($labels['vhost'], $labels['queue']);

                continue;
            }

            $metrics->setQueueCount($labels['vhost'], $labels['queue'], $field, (int) $value);
        }

        return $metrics;
    }

    /**
     * @return array{0: array<string, string>, 1: float}
     *
     * @throws PrometheusMetricsException
     */
    private function parseSample(string $line): array
    {
        if (! preg_match(self::SAMPLE_PATTERN, $line, $sample) || ! is_numeric($sample[2])) {
            throw new PrometheusMetricsException('Malformed Prometheus sample: '.$line);
        }

        preg_match_all(self::LABEL_PATTERN, $sample[1], $matches, PREG_SET_ORDER);

        $labels = [];
        foreach ($matches as [, $labelName, $labelValue]) {
            $labels[$labelName] = strtr($labelValue, self::LABEL_ESCAPES);
        }

        return [$labels, (float) $sample[2]];
    }

    /**
     * @return Generator<string>
     */
    private function readLines(StreamInterface $body): Generator
    {
        if ($body->isSeekable()) {
            $body->rewind();
        }

        $buffer = '';
        while (! $body->eof()) {
            $buffer .= $body->read(self::READ_CHUNK_SIZE);

            $lines = explode("\n", $buffer);
            $buffer = array_pop($lines);

            yield from $lines;
        }

        if ($buffer !== '') {
            yield $buffer;
        }

        $body->close();
    }
}
