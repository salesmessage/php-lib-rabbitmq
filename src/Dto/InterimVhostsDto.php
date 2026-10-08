<?php

namespace Salesmessage\LibRabbitMQ\Dto;

class InterimVhostsDto
{
    /**
     * @param  array<VhostApiDto>  $vhosts
     * @param  array<string, array<string>>  $uncountedQueues  vhost => queues whose message counts were not read
     */
    public function __construct(
        private array $vhosts,
        private array $uncountedQueues = []
    ) {}

    /**
     * @return array<VhostApiDto>
     */
    public function getVhosts(): array
    {
        return $this->vhosts;
    }

    /**
     * @return array<string, array<string>>
     */
    public function getUncountedQueues(): array
    {
        return $this->uncountedQueues;
    }
}
