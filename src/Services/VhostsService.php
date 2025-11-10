<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Throwable;

class VhostsService
{
    /**
     * @param RabbitApiClient $rabbitApiClient
     * @param LoggerInterface $logger
     */
    public function __construct(
        private RabbitApiClient $rabbitApiClient,
        private LoggerInterface $logger
    )
    {
        $connectionConfig = (array) config('queue.connections.rabbitmq_vhosts', []);

        $this->rabbitApiClient->setConnectionConfig($connectionConfig);
    }

    public function getAllVhosts(int $fromPage = 1): \Generator
    {
        while (true) {
            $data = $this->rabbitApiClient->request(
                'GET',
                '/api/vhosts', [
                'page' => $fromPage,
                'page_size' => 500,
                'columns' => 'name,messages,messages_ready,messages_unacknowledged',
            ]);

            $items = $data['items'] ?? [];
            if (!is_array($items) || !isset($data['page_count'])) {
                throw new \LogicException('Unexpected response from RabbitMQ API');
            }
            if (empty($items)) {
                break;
            }

            foreach ($items as $item) {
                yield $item;
            }

            $nextPage = $fromPage + 1;
            $totalPages = (int) $data['page_count'];
            if ($nextPage > $totalPages) {
                break;
            }

            $fromPage = $nextPage;
        }
    }

    /**
     * @param int $organizationId
     * @return array
     */
    public function getVhostForOrganization(int $organizationId): array
    {
        $vhostName = $this->getVhostName($organizationId);

        return $this->getVhost($vhostName);
    }

    /**
     * @param string $vhostName
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     */
    public function getVhost(string $vhostName): array
    {
        try {
            $data = $this->rabbitApiClient->request(
                'GET',
                '/api/vhosts/' . $vhostName,
                [
                    'columns' => 'name,messages,messages_ready,messages_unacknowledged',
                ]
            );
        } catch (Throwable $exception) {
            $this->logger->warning('Salesmessage.LibRabbitMQ.Services.VhostsService.getVhost.exception', [
                'vhost_name' => $vhostName,
                'message' => $exception->getMessage(),
                'code' => $exception->getCode(),
                'trace' => $exception->getTraceAsString(),
            ]);

            $data = [];
        }

        return $data;
    }

    /**
     * @param int $organizationId
     * @return bool
     */
    public function createVhostForOrganization(int $organizationId): bool
    {
        $vhostName = $this->getVhostName($organizationId);
        $description = $this->getVhostDescription($organizationId);

        return $this->createVhost($vhostName, $description);
    }

    /**
     * @param string $vhostName
     * @param string $description
     * @return bool
     * @throws \GuzzleHttp\Exception\GuzzleException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     */
    public function createVhost(string $vhostName, string $description): bool
    {
        try {
            $this->rabbitApiClient->request(
                'PUT',
                '/api/vhosts/' . $vhostName,
                [],
                [
                    'description' => $description,
                    'default_queue_type' => 'classic',
                ]
            );
            $isCreated = true;
        } catch (Throwable $exception) {
            $this->logger->warning('Salesmessage.LibRabbitMQ.Services.VhostsService.createVhost.exception', [
                'vhost_name' => $vhostName,
                'message' => $exception->getMessage(),
                'code' => $exception->getCode(),
                'trace' => $exception->getTraceAsString(),
            ]);

            $isCreated = false;
        }

        $isSuccess = false;
        if ($isCreated) {
            $isSuccess = $this->setVhostPermissions($vhostName);
        }

        return $isSuccess;
    }

    /**
     * @param string $vhostName
     * @param string $userName
     * @return bool
     */
    public function setVhostPermissions(string $vhostName): bool
    {
        try {
            $this->rabbitApiClient->request(
                'PUT',
                '/api/permissions/' . $vhostName . '/' . $this->rabbitApiClient->getUsername(),
                [],
                [
                    'configure' => '.*',
                    'write' => '.*',
                    'read' => '.*',
                ]
            );
            $isSuccess = true;
        } catch (Throwable $exception) {
            $this->logger->warning('Salesmessage.LibRabbitMQ.Services.VhostsService.setVhostPermissions.exception', [
                'vhost_name' => $vhostName,
                'message' => $exception->getMessage(),
                'code' => $exception->getCode(),
                'trace' => $exception->getTraceAsString(),
            ]);

            $isSuccess = false;
        }

        return $isSuccess;
    }

    /**
     * @param int $organizationId
     * @return string
     */
    public function getVhostName(int $organizationId): string
    {
        $vhostPrefix = config('queue.connections.rabbitmq_vhosts.vhost_prefix', 'organization_');

        return $vhostPrefix . $organizationId;
    }

    /**
     * @param int $organizationId
     * @return string
     */
    private function getVhostDescription(int $organizationId): string
    {
        return sprintf('Vhost for organization ID: %d', $organizationId);
    }
}

