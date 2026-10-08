<?php

namespace Salesmessage\LibRabbitMQ\Services\InterimVhosts;

use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class ManagementApiVhostsSource implements InterimVhostsSourceInterface
{
    public function __construct(private VhostsService $vhostsService) {}

    /**
     * @return $this
     */
    public function setConnection(string $connectionName): self
    {
        $this->vhostsService->setConnection($connectionName);

        return $this;
    }

    /**
     * @return array<VhostApiDto>
     *
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function getVhosts(): array
    {
        $vhosts = [];
        foreach ($this->vhostsService->getAllVhosts() as $vhostApiData) {
            $vhostDto = new VhostApiDto($vhostApiData);
            if ($vhostDto->getName() !== '') {
                $vhosts[] = $vhostDto;
            }
        }

        return $vhosts;
    }
}
