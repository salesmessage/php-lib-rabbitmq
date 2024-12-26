<?php

namespace Salesmessage\LibRabbitMQ\Queue;

use PhpAmqpLib\Connection\AbstractConnection;
use Salesmessage\LibRabbitMQ\Dto\ConnectionNameDto;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Channel\AMQPChannel;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class RabbitMQQueueBatchable extends BaseRabbitMQQueue
{
    private InternalStorageManager $internalStorageManager;

    private GroupsService $groupsService;

    private VhostsService $vhostsService;

    /**
     * @param QueueConfig $config
     */
    public function __construct(QueueConfig $config)
    {
        $this->internalStorageManager = app(InternalStorageManager::class);
        $this->groupsService = app(GroupsService::class);
        $this->vhostsService = app(VhostsService::class);

        parent::__construct($config);
    }

    protected function publishBasic(
        $msg,
        $exchange = '',
        $destination = '',
        $mandatory = false,
        $immediate = false,
        $ticket = null
    ): void
    {
        try {
            parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket);
        } catch (AMQPConnectionClosedException|AMQPChannelClosedException) {
            $this->reconnect();
            parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket);
        }
    }

    protected function publishBatch($jobs, $data = '', $queue = null): void
    {
        try {
            parent::publishBatch($jobs, $data, $queue);
        } catch (AMQPConnectionClosedException|AMQPChannelClosedException) {
            $this->reconnect();
            parent::publishBatch($jobs, $data, $queue);
        }
    }

    protected function createChannel(): AMQPChannel
    {
        try {
            return parent::createChannel();
        } catch (AMQPConnectionClosedException) {
            $this->reconnect();
            return parent::createChannel();
        }
    }

    public function push($job, $data = '', $queue = null)
    {
        $queue = $queue ?: $job->onQueue();

        try {
            $result = parent::push($job, $data, $queue);
        } catch (AMQPConnectionClosedException $exception) {
            if (530 !== $exception->getCode()) {
                throw $exception;
            }

            // vhost not found
            if (false === $this->createNotExistsVhost()) {
                throw $exception;
            }

            $result = parent::push($job, $data, $queue);
        }

        if (config('queue.connections.rabbitmq_vhosts.immediate_indexation')) {
            $this->addQueueToIndex((string) $queue);
        }

        return $result;
    }

    public function pushRaw($payload, $queue = null, array $options = []): int|string|null
    {
        return parent::pushRaw($payload, $queue, $options);
    }

    /**
     * @return bool
     * @throws \GuzzleHttp\Exception\GuzzleException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     */
    private function createNotExistsVhost(): bool
    {
        $dto = new ConnectionNameDto($this->getConnectionName());
        if (null === $dto->getVhostName()) {
            return false;
        }

        return $this->vhostsService->createVhost($dto->getVhostName(), 'Automatically created vhost');
    }

    /**
     * @param string $queue
     * @return bool
     */
    private function addQueueToIndex(string $queue): bool
    {
        if ('' === $queue) {
            return false;
        }

        $dto = new ConnectionNameDto($this->getConnectionName());
        $vhostName = $dto->getVhostName();
        if (null === $vhostName) {
            return false;
        }

        $groups = $this->groupsService->getAllGroupsNames();

        $queueApiDto = new QueueApiDto([
            'name' => $queue,
            'vhost' => $vhostName,
        ]);
        $isQueueActivated = $this->internalStorageManager->activateQueue($queueApiDto, $groups);

        $vhostDto = new VhostApiDto([
            'name' => $vhostName,
        ]);
        $isVhostActivated = $this->internalStorageManager->activateVhost($vhostDto, $groups);

        return $isQueueActivated && $isVhostActivated;
    }
}

