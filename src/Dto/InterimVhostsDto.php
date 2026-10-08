<?php

namespace Salesmessage\LibRabbitMQ\Dto;

class InterimVhostsDto
{
    /**
     * @param  array<VhostApiDto>  $vhosts
     * @param  array<string>  $skippedNodeNames  nodes left out of the counts, so vhosts missing from $vhosts may still exist
     */
    public function __construct(
        private array $vhosts,
        private array $skippedNodeNames = []
    ) {}

    /**
     * @return array<VhostApiDto>
     */
    public function getVhosts(): array
    {
        return $this->vhosts;
    }

    /**
     * @return array<string>
     */
    public function getSkippedNodeNames(): array
    {
        return $this->skippedNodeNames;
    }
}
