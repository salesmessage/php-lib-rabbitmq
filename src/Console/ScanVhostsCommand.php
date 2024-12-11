<?php

namespace Salesmessage\LibRabbitMQ\Console;

use Illuminate\Console\Command;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Redis;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Throwable;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;

class ScanVhostsCommand extends Command
{
    protected $signature = 'lib-rabbitmq:scan-vhosts
                            {connection? : The name of the queue connection to work}';

    protected $description = 'Scan and index vhosts';

    private Collection $vhosts;

    private Collection $vhostQueues;

    /**
     * @param RabbitApiClient $apiClient
     * @param InternalStorageManager $internalStorageManager
     */
    public function __construct(
        private RabbitApiClient $apiClient,
        private InternalStorageManager $internalStorageManager
    ) {
        $this->vhosts = new Collection();
        $this->vhostQueues = new Collection();

        parent::__construct();
    }

    /**
     * @return int
     */
    public function handle()
    {
        $connectionName = (string) ($this->argument('connection') ?: $this->laravel['config']['queue.default']);

        $connectionConfig = $this->laravel['config']['queue']['connections'][$connectionName] ?? [];
        if (empty($connectionConfig)) {
            $this->error(sprintf('Config for connection "%s" not found.', $connectionName));
            return Command::INVALID;
        }

        $this->apiClient->setConnectionConfig($connectionConfig);

        $this->loadVhosts();

        $oldVhosts = $this->internalStorageManager->getVhosts();

        if ($this->vhosts->isNotEmpty()) {
            foreach ($this->vhosts as $vhost) {
                $vhostDto = $this->processVhost($vhost);
                if (null === $vhostDto) {
                    continue;
                }

                $oldVhostIndex = array_search($vhostDto->getName(), $oldVhosts, true);
                if (false !== $oldVhostIndex) {
                    unset($oldVhosts[$oldVhostIndex]);
                }
            }
        } else {
            $this->warn(sprintf('Vhosts for connection "%s" not found.', $connectionName));
        }

        $this->removeOldsVhosts($oldVhosts);

        return Command::SUCCESS;
    }

    /**
     * @param int $page
     * @param int $pageSize
     * @return void
     */
    private function loadVhosts(int $page = 1, int $pageSize = 500): void
    {
        try {
            $data = $this->apiClient->request(
                'GET',
                '/api/vhosts', [
                'page' => $page,
                'page_size' => $pageSize,
                'columns' => 'name,messages,messages_ready,messages_unacknowledged',
            ]);
        } catch (Throwable $exception) {
            $data = [];

            $this->error(sprintf('Load vhosts error: %s.', (string) $exception->getMessage()));
        }

        $items = (array) ($data['items'] ?? []);
        if (!empty($items)) {
            $this->vhosts->push(...$items);
        }

        $nextPage = $page + 1;
        $lastPage = (int) ($data['page_count'] ?? 1);
        if ($lastPage >= $nextPage) {
            $this->loadVhosts($nextPage, $pageSize);
            return;
        }
    }

    /**
     * @param array $vhostApiData
     * @return VhostApiDto|null
     */
    private function processVhost(array $vhostApiData): ?VhostApiDto
    {
        $vhostDto = new VhostApiDto($vhostApiData);
        if ('' === $vhostDto->getName()) {
            return null;
        }

        $indexedSuccessfully = $this->internalStorageManager->indexVhost($vhostDto);
        if (!$indexedSuccessfully) {
            $this->warn(sprintf(
                'Skip indexation vhost: "%s". Messages ready: %d.',
                $vhostDto->getName(),
                $vhostDto->getMessagesReady()
            ));

            return null;
        }

        $this->info(sprintf(
            'Successfully indexed vhost: "%s". Messages ready: %d.',
            $vhostDto->getName(),
            $vhostDto->getMessagesReady()
        ));

        $this->vhostQueues = new Collection();

        $this->loadVhostQueues($vhostDto);

        $oldVhostQueues = $this->internalStorageManager->getVhostQueues($vhostDto->getName());

        if ($this->vhostQueues->isNotEmpty()) {
            foreach ($this->vhostQueues as $queueApiData) {
                $processQueueDto = $this->processVhostQueue($queueApiData);
                if (null === $processQueueDto) {
                    continue;
                }

                $oldVhostQueueIndex = array_search($processQueueDto->getName(), $oldVhostQueues, true);
                if (false !== $oldVhostQueueIndex) {
                    unset($oldVhostQueues[$oldVhostQueueIndex]);
                }
            }
        } else {
            $this->warn(sprintf(
                'Queues for vhost "%s" not found.',
                $vhostDto->getName()
            ));
        }

        $this->removeOldVhostQueues($vhostDto, $oldVhostQueues);

        return $vhostDto;
    }

    /**
     * @param array $oldVhosts
     * @return void
     */
    private function removeOldsVhosts(array $oldVhosts): void
    {
        if (empty($oldVhosts)) {
            return;
        }

        foreach ($oldVhosts as $oldVhostName) {
            $vhostDto = new VhostApiDto([
                'name' => $oldVhostName,
            ]);

            $this->internalStorageManager->removeVhost($vhostDto);

            $this->warn(sprintf(
                'Removed from index vhost: "%s".',
                $vhostDto->getName()
            ));
        }
    }

    /**
     * @param VhostApiDto $vhostDto
     * @param int $page
     * @param int $pageSize
     * @return void
     */
    private function loadVhostQueues(VhostApiDto $vhostDto, int $page = 1, int $pageSize = 500): void
    {
        try {
            $data = $this->apiClient->request(
                'GET',
                '/api/queues/' . $vhostDto->getApiName(), [
                'page' => $page,
                'page_size' => $pageSize,
                'columns' => 'name,vhost,messages,messages_ready,messages_unacknowledged',
                'disable_stats' => 'true',
                'enable_queue_totals' => 'true',
            ]);
        } catch (Throwable $exception) {
            $data = [];

            $this->error(sprintf(
                'Load vhost "%s" queues error: %s.',
                $vhostDto->getName(),
                (string) $exception->getMessage()
            ));
        }

        $items = (array) ($data['items'] ?? []);
        if (!empty($items)) {
            $this->vhostQueues->push(...$items);
        }

        $nextPage = $page + 1;
        $lastPage = (int) ($data['page_count'] ?? 1);
        if ($lastPage >= $nextPage) {
            $this->loadVhostQueues($vhostDto, $nextPage, $pageSize);
            return;
        }
    }

    /**
     * @param array $queueApiData
     * @return void
     */
    private function processVhostQueue(array $queueApiData): ?QueueApiDto
    {
        $queueApiDto = new QueueApiDto($queueApiData);
        if ('' === $queueApiDto->getName()) {
            return null;
        }

        $indexedSuccessfully = $this->internalStorageManager->indexQueue($queueApiDto);
        if (!$indexedSuccessfully) {
            $this->warn(sprintf(
                'Skip indexation queue: "%s". Vhost: %s. Messages ready: %d.',
                $queueApiDto->getName(),
                $queueApiDto->getVhostName(),
                $queueApiDto->getMessagesReady()
            ));

            return null;
        }

        $this->info(sprintf(
            'Successfully indexed queue: "%s". Vhost: %s. Messages ready: %d.',
            $queueApiDto->getName(),
            $queueApiDto->getVhostName(),
            $queueApiDto->getMessagesReady()
        ));

        return $queueApiDto;
    }

    /**
     * @param VhostApiDto $vhostDto
     * @param array $oldVhostQueues
     * @return void
     */
    private function removeOldVhostQueues(VhostApiDto $vhostDto, array $oldVhostQueues): void
    {
        if (empty($oldVhostQueues)) {
            return;
        }

        foreach ($oldVhostQueues as $oldQueueName) {
            $queueApiDto = new QueueApiDto([
                'name' => $oldQueueName,
                'vhost' => $vhostDto->getName(),
            ]);

            $this->internalStorageManager->removeQueue($queueApiDto);

            $this->warn(sprintf(
                'Removed from index queue: "%s". Vhost: %s.',
                $queueApiDto->getName(),
                $queueApiDto->getVhostName()
            ));
        }
    }
}

