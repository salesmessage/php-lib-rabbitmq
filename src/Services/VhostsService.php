<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Illuminate\Support\Collection;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Throwable;

class VhostsService
{
    public const VHOST_PREFIX = 'organization_';

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

    /**
     * @param int $page
     * @param int $pageSize
     * @param Collection|null $vhosts
     * @return Collection
     */
    public function getAllVhosts(
        int $page = 1, 
        int $pageSize = 500,
        ?Collection $vhosts = null,
    ): Collection
    {
        if (null === $vhosts) {
            $vhosts = new Collection();
        }
        
        try {
            $data = $this->rabbitApiClient->request(
                'GET',
                '/api/vhosts', [
                'page' => $page,
                'page_size' => $pageSize,
                'columns' => 'name,messages,messages_ready,messages_unacknowledged',
            ]);
        } catch (Throwable $exception) {
            $this->logger->warning('Salesmessage.LibRabbitMQ.Services.VhostsService.getAllVhosts.exception', [
                'message' => $exception->getMessage(),
                'code' => $exception->getCode(),
                'trace' => $exception->getTraceAsString(),
            ]);

            $data = [];
        }

        $items = (array) ($data['items'] ?? []);
        if (!empty($items)) {
            $vhosts->push(...$items);
        }

        $nextPage = $page + 1;
        $lastPage = (int) ($data['page_count'] ?? 1);
        if ($lastPage >= $nextPage) {
            return $this->getAllVhosts($nextPage, $pageSize, $vhosts);
        }

        return $vhosts;
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

        return $isCreated;
    }

    /**
     * @param int $organizationId
     * @return string
     */
    public function getVhostName(int $organizationId): string
    {
        return self::VHOST_PREFIX . $organizationId;
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

