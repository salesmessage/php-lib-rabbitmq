<?php

namespace Salesmessage\LibRabbitMQ\Queue;

use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;
use Salesmessage\LibRabbitMQ\Dto\ConnectionNameDto;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Channel\AMQPChannel;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Lock\LockService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class RabbitMQQueueBatchable extends BaseRabbitMQQueue
{
    private InternalStorageManager $internalStorageManager;

    private GroupsService $groupsService;

    private VhostsService $vhostsService;

    private LockService $lockService;

    private LoggerInterface $logger;

    /**
     * @param QueueConfig $config
     */
    public function __construct(QueueConfig $config)
    {
        $this->internalStorageManager = app(InternalStorageManager::class);
        $this->groupsService = app(GroupsService::class);
        $this->vhostsService = app(VhostsService::class);
        $this->lockService = app(LockService::class);
        $this->logger = app(LoggerInterface::class);

        parent::__construct($config);
    }

    protected function publishBasic(
        $msg,
        $exchange = '',
        $destination = '',
        $mandatory = false,
        $immediate = false,
        $ticket = null
    ): void {
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
        } catch (AMQPConnectionClosedException $exception) {
            if ($this->isVhostFailedException($exception) && (false === $this->createNotExistsVhost())) {
                throw $exception;
            }

            $this->reconnect();
            return parent::createChannel();
        }
    }

    public function push($job, $data = '', $queue = null)
    {
        if (!($job instanceof RabbitMQConsumable)) {
            throw new \InvalidArgumentException('Job must implement RabbitMQConsumable');
        }

        if (!$queue) {
            if (!method_exists($job, 'onQueue')) {
                throw new \InvalidArgumentException('Job must implement onQueue method');
            }
            $queue = $job->onQueue();
        }

        if ($job->getQueueType() === RabbitMQConsumable::MQ_TYPE_QUORUM) {
            $queue .= $this->getConfig()->getQuorumQueuePostfix();
        }

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

    private function createNotExistsVhost(int $attempts = 0): bool
    {
        $dto = new ConnectionNameDto($this->getConnectionName());
        if (null === $dto->getVhostName()) {
            return false;
        }

        $hasCreated = false;
        $creationHandler = function () use ($dto, &$hasCreated) {
            $hasCreated = $this->vhostsService->createVhost($dto->getVhostName(), 'Automatically created vhost');
        };

        try {
            $lockKey = 'vhost:' . $dto->getVhostName() . ':creation';
            $handlerWasRun = $this->lockService->lock($lockKey, $creationHandler, skipHandlingOnLock: true);
            // if handler was not run, it means that another process has possibly created the vhost
            // and we need to just re-check if it exists
            if (!$handlerWasRun) {
                $hasCreated = isset($this->vhostsService->getVhost($dto->getVhostName(), 'name')['name']);
            }
        } catch (\Throwable $e) {
            $this->logger->error('RabbitMQQueueBatchable.createNotExistsVhost.exception', [
                'vhost_name' => $dto->getVhostName(),
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
        }

        if (!$hasCreated && $attempts < 2) {
            sleep(1);
            return $this->createNotExistsVhost(++$attempts);
        }

        return $hasCreated;
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

    /**
     * @param AMQPConnectionClosedException $exception
     * @return bool
     */
    private function isVhostFailedException(AMQPConnectionClosedException $exception): bool
    {
        $dto = new ConnectionNameDto($this->getConnectionName());
        $vhostName = (string) $dto->getVhostName();

        $notFoundErrorMessage = sprintf('NOT_ALLOWED - vhost %s not found', $vhostName);
        if ((403 === $exception->getCode()) && str_contains($exception->getMessage(), $notFoundErrorMessage)) {
            return true;
        }

        $deletedErrorMessage = sprintf('CONNECTION_FORCED - vhost \'%s\' is deleted', $vhostName);
        if (str_contains($exception->getMessage(), $deletedErrorMessage)) {
            return true;
        }

        return false;
    }
}
