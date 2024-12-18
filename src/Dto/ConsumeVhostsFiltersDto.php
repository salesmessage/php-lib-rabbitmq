<?php

namespace Salesmessage\LibRabbitMQ\Dto;

class ConsumeVhostsFiltersDto
{
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
     * @param string $vhosts
     * @param string $vhostsMask
     * @param string $queues
     * @param string $queuesMask
     */
    public function __construct(
        string $vhosts,
        string $vhostsMask,
        string $queues,
        string $queuesMask
    )
    {
        $this->vhosts = $this->stringToArray($vhosts);
        $this->vhostsMask = trim($vhostsMask);
        
        $this->queues = $this->stringToArray($queues);
        $this->queuesMask = trim($queuesMask);
    }

    /**
     * @param string $string
     * @return array
     */
    private function stringToArray(string $string): array
    {
        if ('' === $string) {
            return [];
        }

        $array = explode(',', $string);
        $array = array_map(fn($value) => trim($value), $array);

        return array_filter($array);
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

