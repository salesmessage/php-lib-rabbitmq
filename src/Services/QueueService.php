<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Illuminate\Support\Collection;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Throwable;

class QueueService
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
     * @param VhostApiDto $vhostDto
     * @param int $page
     * @param int $pageSize
     * @param Collection|null $queues
     * @return Collection|null
     * @throws \GuzzleHttp\Exception\GuzzleException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     */
    public function getAllVhostQueues(
        VhostApiDto $vhostDto,
        int $page = 1,
        int $pageSize = 500,
        ?Collection $queues = null,
    ): ?Collection
    {
        if (null === $queues) {
            $queues = new Collection();
        }

        try {
            $data = $this->rabbitApiClient->request(
                'GET',
                '/api/queues/' . $vhostDto->getApiName(), [
                'page' => $page,
                'page_size' => $pageSize,
                'columns' => 'name,vhost,messages,messages_ready,messages_unacknowledged',
                'disable_stats' => 'true',
                'enable_queue_totals' => 'true',
            ]);
        } catch (Throwable $exception) {
            $this->logger->warning('Salesmessage.LibRabbitMQ.Services.QueueService.getAllVhostQueues.exception', [
                'message' => $exception->getMessage(),
                'code' => $exception->getCode(),
                'trace' => $exception->getTraceAsString(),
            ]);

            $data = [];
        }

        $items = (array) ($data['items'] ?? []);
        if (!empty($items)) {
            $queues->push(...$items);
        }

        $nextPage = $page + 1;
        $lastPage = (int) ($data['page_count'] ?? 1);
        if ($lastPage >= $nextPage) {
            return $this->getAllVhostQueues($vhostDto, $nextPage, $pageSize, $queues);
        }

        return $queues;
    }
}

