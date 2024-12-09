<?php

namespace Salesmessage\LibRabbitMQ\Dto;

class ConsumeVhostsFiltersDto
{
    /**
     * @var array
     */
    private array $vhosts = [];

    /**
     * @var array
     */
    private array $queues = [];

    /**
     * @param string $vhosts
     * @param string $queues
     */
    public function __construct(string $vhosts, string $queues)
    {
        $this->vhosts = $this->stringToArray($vhosts);
        $this->queues = $this->stringToArray($queues);
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
}

