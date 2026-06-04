<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Throwable;

class VhostsService
{
    /**
     * @var string
     */
    private string $connectionName = 'rabbitmq_vhosts';

    /**
     * @param RabbitApiClient $rabbitApiClient
     * @param LoggerInterface $logger
     */
    public function __construct(
        private RabbitApiClient $rabbitApiClient,
        private LoggerInterface $logger
    )
    {
        $this->setConnection($this->connectionName);
    }

    /**
     * @param string $connectionName
     * @return $this
     */
    public function setConnection(string $connectionName): self
    {
        $this->connectionName = $connectionName;

        $connectionConfig = (array) config('queue.connections.' . $this->connectionName, []);
        $this->rabbitApiClient->setConnectionConfig($connectionConfig);

        return $this;
    }

    /**
     * @param int $fromPage
     * @return \Generator
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     */
    public function getAllVhosts(int $fromPage = 1): \Generator
    {
        $isSupportPagination = false === $this->rabbitApiClient->isVersionCorrespond(4);

        while (true) {
            $queryParams = [
                'columns' => 'name,messages,messages_ready,messages_unacknowledged',
                'sort' => 'name',
                'sort_reverse' => 'false',
            ];
            if ($isSupportPagination) {
                $queryParams['page'] = $fromPage;
                $queryParams['page_size'] = 500;
            }

            $data = $this->rabbitApiClient->request(
                'GET',
                '/api/vhosts',
                $queryParams
            );

            $items = $isSupportPagination ? ($data['items'] ?? []) : $data;
            $totalPages = $isSupportPagination ? (int) ($data['page_count'] ?? 0) : 1;
            if (!is_array($items) || !$totalPages) {
                throw new \LogicException(sprintf(
                    'Unexpected response from RabbitMQ API: %s',
                    $this->rabbitApiClient->getManagementVersion()
                ));
            }
            if (empty($items)) {
                break;
            }

            foreach ($items as $item) {
                yield $item;
            }

            $nextPage = $fromPage + 1;
            if ($nextPage > $totalPages) {
                break;
            }

            $fromPage = $nextPage;
        }
    }

    /**
     * @param int $organizationId
     * @return array
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
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
    public function getVhost(
        string $vhostName,
        string $columns = 'name,messages,messages_ready,messages_unacknowledged',
    ): array {
        try {
            $data = $this->rabbitApiClient->request(
                'GET',
                '/api/vhosts/' . $vhostName,
                [
                    'columns' => $columns,
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
        } catch (Throwable $exception) {
            $this->logger->warning('Salesmessage.LibRabbitMQ.Services.VhostsService.createVhost.exception', [
                'vhost_name' => $vhostName,
                'message' => $exception->getMessage(),
                'code' => $exception->getCode(),
                'trace' => $exception->getTraceAsString(),
            ]);

            return false;
        }

        return $this->setVhostPermissions($vhostName);
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
        } catch (Throwable $exception) {
            $this->logger->warning('Salesmessage.LibRabbitMQ.Services.VhostsService.setVhostPermissions.exception', [
                'vhost_name' => $vhostName,
                'message' => $exception->getMessage(),
                'code' => $exception->getCode(),
                'trace' => $exception->getTraceAsString(),
            ]);
            return false;
        }

        return true;
    }

    /**
     * @param string $vhostName
     * @return bool
     */
    public function removeVhost(string $vhostName): bool
    {
        try {
            $this->rabbitApiClient->request(
                'DELETE',
                '/api/vhosts/' . $vhostName,
            );
        } catch (Throwable $exception) {
            $this->logger->warning('Salesmessage.LibRabbitMQ.Services.VhostsService.removeVhost.exception', [
                'vhost_name' => $vhostName,
                'message' => $exception->getMessage(),
                'code' => $exception->getCode(),
                'trace' => $exception->getTraceAsString(),
            ]);

            return false;
        }

        return true;
    }

    /**
     * @param string $vhostName
     * @return int|null
     */
    public function getOrganizationIdByVhostName(string $vhostName): ?int
    {
        $vhostPrefix = $this->getVhostPrefix();
        if (('/' === $vhostName)
            || ('' === $vhostName)
            || (false === str_starts_with($vhostName, $vhostPrefix))
        ) {
            return null;
        }

        $organizationId = str_replace($vhostPrefix, '', $vhostName);
        if (false === is_numeric($organizationId)) {
            return null;
        }

        return (int) $organizationId;
    }

    /**
     * @return string
     */
    public function getVhostPrefix(): string
    {
        return (string) config('queue.connections.' . $this->connectionName . '.vhost_prefix', 'organization_');
    }

    /**
     * @param int $organizationId
     * @return string
     */
    public function getVhostName(int $organizationId): string
    {
        return $this->getVhostPrefix() . $organizationId;
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

