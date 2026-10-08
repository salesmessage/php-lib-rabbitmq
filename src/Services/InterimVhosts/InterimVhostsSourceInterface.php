<?php

namespace Salesmessage\LibRabbitMQ\Services\InterimVhosts;

use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;

interface InterimVhostsSourceInterface
{
    /**
     * @return $this
     */
    public function setConnection(string $connectionName): self;

    /**
     * @return array<VhostApiDto>
     *
     * @throws \Throwable
     */
    public function getVhosts(): array;
}
