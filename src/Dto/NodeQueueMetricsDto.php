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
     * @var array<string, array<string, true>> vhost => queue => true
     */
    private array $queueMembers = [];

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

    /**
     * @return $this
     */
    public function addQueueMember(string $vhost, string $queue): self
    {
        $this->queueMembers[$vhost][$queue] = true;

        return $this;
    }

    /**
     * @return array<string, array<string, true>>
     */
    public function getQueueMembers(): array
    {
        return $this->queueMembers;
    }
}
