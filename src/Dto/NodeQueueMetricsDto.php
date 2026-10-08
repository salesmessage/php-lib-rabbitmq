<?php

namespace Salesmessage\LibRabbitMQ\Dto;

class NodeQueueMetricsDto
{
    private ?string $nodeName = null;

    private ?string $clusterId = null;

    /**
     * @var array<string, array<string, array<string, int>>> vhost => queue => field => count
     */
    private array $queueCounts = [];

    /**
     * @return $this
     */
    public function setIdentity(?string $nodeName, ?string $clusterId): self
    {
        $this->nodeName = $nodeName;
        $this->clusterId = $clusterId;

        return $this;
    }

    public function getNodeName(): ?string
    {
        return $this->nodeName;
    }

    public function getClusterId(): ?string
    {
        return $this->clusterId;
    }

    /**
     * @return $this
     */
    public function setQueueCount(string $vhost, string $queue, string $field, int $count): self
    {
        $this->queueCounts[$vhost][$queue][$field] = $count;

        return $this;
    }

    /**
     * @return array<string, array<string, array<string, int>>>
     */
    public function getQueueCounts(): array
    {
        return $this->queueCounts;
    }
}
