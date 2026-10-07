<?php

namespace Salesmessage\LibRabbitMQ\Services\QueueExpiry;

use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Throwable;

class QueueExpiryPolicyService
{
    private ?QueueExpiryPolicyOptions $options = null;

    public function __construct(
        private RabbitApiClient $rabbitApiClient,
        private LoggerInterface $logger
    ) {
        $this->setConnection('rabbitmq_vhosts');
    }

    public function setConnection(string $connectionName): self
    {
        $this->rabbitApiClient->setConnectionConfig((array) config('queue.connections.'.$connectionName, []));

        return $this;
    }

    /**
     * @throws \InvalidArgumentException
     */
    public function getOptions(): QueueExpiryPolicyOptions
    {
        return $this->options ??= QueueExpiryPolicyOptions::fromConfig(
            (array) config('queue.drivers.rabbitmq_vhosts.queue_expiry', [])
        );
    }

    public function applyIfEnabled(string $vhostName): bool
    {
        try {
            $isEnabled = $this->getOptions()->isEnabled();
        } catch (\InvalidArgumentException $exception) {
            $this->logger->error('Salesmessage.LibRabbitMQ.Services.QueueExpiryPolicyService.applyIfEnabled.invalid_config', [
                'vhost_name' => $vhostName,
                'message' => $exception->getMessage(),
            ]);

            return false;
        }

        if ($isEnabled === false) {
            return false;
        }

        return $this->apply($vhostName);
    }

    public function apply(string $vhostName): bool
    {
        try {
            $this->rabbitApiClient->request('PUT', $this->policyUri($vhostName), [], $this->getOptions()->toPolicyBody());
        } catch (Throwable $exception) {
            $this->logger->error('Salesmessage.LibRabbitMQ.Services.QueueExpiryPolicyService.apply.exception', [
                'vhost_name' => $vhostName,
                'policy_name' => $this->getOptions()->getName(),
                'message' => $exception->getMessage(),
            ]);

            return false;
        }

        return true;
    }

    public function remove(string $vhostName): bool
    {
        try {
            $this->rabbitApiClient->request('DELETE', $this->policyUri($vhostName));
        } catch (Throwable $exception) {
            $this->logger->error('Salesmessage.LibRabbitMQ.Services.QueueExpiryPolicyService.remove.exception', [
                'vhost_name' => $vhostName,
                'policy_name' => $this->getOptions()->getName(),
                'message' => $exception->getMessage(),
            ]);

            return false;
        }

        return true;
    }

    /**
     * @return array<string, array> this policy keyed by vhost name
     *
     * @throws \GuzzleHttp\Exception\GuzzleException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     */
    public function getPoliciesByVhost(): array
    {
        $policies = [];
        foreach ($this->rabbitApiClient->request('GET', '/api/operator-policies') as $policy) {
            if (is_array($policy) && ($this->getOptions()->getName() === ($policy['name'] ?? null))) {
                $policies[(string) $policy['vhost']] = $policy;
            }
        }

        return $policies;
    }

    private function policyUri(string $vhostName): string
    {
        return '/api/operator-policies/'.rawurlencode($vhostName).'/'.rawurlencode($this->getOptions()->getName());
    }
}
