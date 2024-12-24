<?php

namespace Salesmessage\LibRabbitMQ\Dto;

class ConsumeVhostsFiltersDto
{
    /**
     * @var string
     */
    private $group = '';

    /**
     * @var array
     */
    private array $vhosts = [];

    /**
     * @var string 
     */
    private string $vhostsMask = '';

    /**
     * @var array
     */
    private array $queues = [];

    /**
     * @var string
     */
    private string $queuesMask = '';

    /**
     * @param string $group
     * @param array $vhosts
     * @param string $vhostsMask
     * @param array $queues
     * @param string $queuesMask
     */
    public function __construct(
        string $group,
        array $vhosts,
        string $vhostsMask,
        array $queues,
        string $queuesMask
    )
    {
        $this->group = trim($group);

        $this->vhosts = array_filter($vhosts);
        $this->vhostsMask = trim($vhostsMask);
        
        $this->queues = array_filter($queues);
        $this->queuesMask = trim($queuesMask);
    }

    /**
     * @return string
     */
    public function getGroup(): string
    {
        return $this->group;
    }

    /**
     * @return array
     */
    public function getVhosts(): array
    {
        return $this->vhosts;
    }

    /**
     * @return bool
     */
    public function hasVhosts(): bool
    {
        return !empty($this->vhosts);
    }

    /**
     * @return string
     */
    public function getVhostsMask(): string
    {
        return $this->vhostsMask;
    }

    /**
     * @return array
     */
    public function getQueues(): array
    {
        return $this->queues;
    }

    /**
     * @return bool
     */
    public function hasQueues(): bool
    {
        return !empty($this->queues);
    }

    /**
     * @return string
     */
    public function getQueuesMask(): string
    {
        return $this->queuesMask;
    }
}

