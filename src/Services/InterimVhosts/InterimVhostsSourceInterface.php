<?php

namespace Salesmessage\LibRabbitMQ\Services\InterimVhosts;

use Salesmessage\LibRabbitMQ\Dto\InterimVhostsDto;

interface InterimVhostsSourceInterface
{
    /**
     * @return $this
     */
    public function setConnection(string $connectionName): self;

    /**
     * @throws \Throwable
     */
    public function getVhosts(): InterimVhostsDto;
}
