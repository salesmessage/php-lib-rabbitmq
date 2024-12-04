<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Console;

use Illuminate\Console\Command;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Redis;
use VladimirYuldashev\LaravelQueueRabbitMQ\Dto\QueueApiDto;
use VladimirYuldashev\LaravelQueueRabbitMQ\Dto\VhostApiDto;
use VladimirYuldashev\LaravelQueueRabbitMQ\Services\Api\RabbitApiClient;
use Throwable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Services\InternalStorageManager;

class ScanVhostsCommand extends Command
{
    protected $signature = 'rabbitmq:scan-vhosts
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
        $this->internalStorageManager->getVhostQueues('/');


        $connectionName = (string) ($this->argument('connection') ?: $this->laravel['config']['queue.default']);

        $connectionConfig = $this->laravel['config']['queue']['connections'][$connectionName] ?? [];
        if (empty($connectionConfig)) {
            $this->error(sprintf('Config for connection "%s" not found.', $connectionName));
            return Command::INVALID;
        }

        $this->apiClient->setConnectionConfig($connectionConfig);

        $this->loadVhosts();

        if ($this->vhosts->isNotEmpty()) {
            foreach ($this->vhosts as $vhost) {
                $this->processVhost($vhost);
            }
        } else {
            $this->warn(sprintf('Vhosts for connection "%s" not found.', $connectionName));
        }

        return Command::SUCCESS;
    }

    /**
     * @param int $page
     * @param int $pageSize
     * @return void
     */
    private function loadVhosts(int $page = 1, int $pageSize = 100): void
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
     * @return void
     */
    private function processVhost(array $vhostApiData): void
    {
        $vhostDto = new VhostApiDto($vhostApiData);
        if ('' === $vhostDto->getName()) {
            return;
        }

        $indexedSuccessfully = $this->internalStorageManager->indexVhost($vhostDto);
        if (!$indexedSuccessfully) {
            $this->warn(sprintf(
                'Skip processing vhost: "%s". Messages ready: %d.',
                $vhostDto->getName(),
                $vhostDto->getMessagesReady()
            ));

            return;
        }

        $this->info(sprintf(
            'Start processing vhost: "%s". Messages ready: %d.',
            $vhostDto->getName(),
            $vhostDto->getMessagesReady()
        ));

        $this->vhostQueues = new Collection();
        $this->loadVhostQueues($vhostDto);

        if ($this->vhostQueues->isNotEmpty()) {
            foreach ($this->vhostQueues as $queueApiData) {
                $this->processVhostQueue($queueApiData);
            }
        } else {
            $this->warn(sprintf(
                'Queues for vhost "%s" not found.',
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
    private function loadVhostQueues(VhostApiDto $vhostDto, int $page = 1, int $pageSize = 2): void
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
    private function processVhostQueue(array $queueApiData): void
    {
        $queueApiDto = new QueueApiDto($queueApiData);
        if ('' === $queueApiDto->getName()) {
            return;
        }

        $indexedSuccessfully = $this->internalStorageManager->indexQueue($queueApiDto);
        if (!$indexedSuccessfully) {
            $this->warn(sprintf(
                'Skip processing queue: "%s". Vhost: %s. Messages ready: %d.',
                $queueApiDto->getName(),
                $queueApiDto->getVhostName(),
                $queueApiDto->getMessagesReady()
            ));

            return;
        }

        $this->info(sprintf(
            'Start processing queue: "%s". Vhost: %s. Messages ready: %d.',
            $queueApiDto->getName(),
            $queueApiDto->getVhostName(),
            $queueApiDto->getMessagesReady()
        ));
    }
}

