<?php

namespace Salesmessage\LibRabbitMQ\Services\InterimVhosts;

use InvalidArgumentException;

class InterimVhostsSourceFactory
{
    public const SOURCE_PROMETHEUS = 'prometheus';

    public const SOURCE_MANAGEMENT = 'management';

    public function __construct(
        private PrometheusVhostsSource $prometheusSource,
        private ManagementApiVhostsSource $managementSource
    ) {}

    public function make(string $connectionName): InterimVhostsSourceInterface
    {
        $source = (string) config(
            'queue.connections.'.$connectionName.'.interim_vhosts_source',
            self::SOURCE_PROMETHEUS
        );

        $vhostsSource = match ($source) {
            self::SOURCE_PROMETHEUS => $this->prometheusSource,
            self::SOURCE_MANAGEMENT => $this->managementSource,
            default => throw new InvalidArgumentException(sprintf('Unknown interim vhosts source "%s"', $source)),
        };

        return $vhostsSource->setConnection($connectionName);
    }
}
