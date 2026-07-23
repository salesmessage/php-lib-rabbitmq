<?php

namespace Salesmessage\LibRabbitMQ\Console;

use Illuminate\Console\Command;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Exceptions\RabbitVhostsGroupsException;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\QueueService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Scheduler\ProcessingTimeSchedulerOptions;
use Salesmessage\LibRabbitMQ\Services\Scheduler\VhostSchedulerFactory;
use Generator;

class ScanVhostsCommand extends Command
{
    protected const TYPE_API = 'api';
    protected const TYPE_INTERIM = 'interim';

    protected $signature = 'lib-rabbitmq:scan-vhosts
                            {--connection=rabbitmq_vhosts : The name of the queue connection to work}
                            {--type=api : Scan type}
                            {--filter= : Vhost name filter}
                            {--sleep=1 : Number of seconds to sleep}
                            {--max-time=0 : Maximum seconds the command can run before stopping}
                            {--with-output=true : Show output details during iteration}
                            {--max-memory=0 : Maximum memory usage in megabytes before stopping}';

    protected $description = 'Scan and index vhosts';

    private array $groups;
    private array $processingTimeGroups = [];
    private bool $silent = false;
    private ?ProcessingTimeSchedulerOptions $schedulerOptions = null;

    /**
     * @param GroupsService $groupsService
     * @param VhostsService $vhostsService
     * @param QueueService $queueService
     * @param InternalStorageManager $internalStorageManager
     */
    public function __construct(
        private GroupsService $groupsService,
        private VhostsService $vhostsService,
        private QueueService $queueService,
        private InternalStorageManager $internalStorageManager
    ) {
        parent::__construct();
    }

    public function handle(): void
    {
        $connectionName = (string) $this->option('connection');

        try {
            $this->groupsService->setConnection($connectionName);
            $groups = $this->groupsService->getAllGroupsNames();
        } catch (RabbitVhostsGroupsException $exception) {
            $this->warn($exception->getMessage());

            $groups = [];
        }

        $this->groups = $groups;

        // window-cost decay only applies to groups scheduled on processing time;
        // groups on last_processed never record any, so refreshing them is dead work
        $this->processingTimeGroups = array_values(array_filter(
            $groups,
            fn (string $groupName): bool => VhostSchedulerFactory::STRATEGY_PROCESSING_TIME === (string) (
                $this->groupsService->getGroupConfig($groupName)['scheduler_strategy'] ?? ''
            )
        ));

        $this->schedulerOptions = ProcessingTimeSchedulerOptions::fromConfig(
            (array) config('queue.drivers.rabbitmq_vhosts.scheduler.strategies.processing_time', [])
        );

        $this->vhostsService->setConnection($connectionName);
        $this->queueService->setConnection($connectionName);
        $this->internalStorageManager->setConnection($connectionName);

        $sleep = (int) $this->option('sleep');
        $maxTime = max(0, (int) $this->option('max-time'));
        $this->silent = !filter_var($this->option('with-output'), FILTER_VALIDATE_BOOLEAN);

        $maxMemoryMb = max(0, (int) $this->option('max-memory'));
        $maxMemoryBytes = $maxMemoryMb > 0 ? $maxMemoryMb * 1024 * 1024 : 0;

        $startedAt = microtime(true);

        $this->line(sprintf('Started... Connection: %s', $connectionName), 'info');

        while (true) {
            $iterationStartedAt = microtime(true);

            $this->processVhosts();

            $iterationEndedAt = microtime(true);

            $iterationDuration = $iterationEndedAt - $iterationStartedAt;
            $totalRuntime = $iterationEndedAt - $startedAt;
            $memoryUsage = memory_get_usage(true);
            $memoryPeakUsage = memory_get_peak_usage(true);

            $this->line(sprintf(
                'Iteration finished in %.2f seconds (total runtime %.2f seconds). Memory usage: %s (peak %s).',
                $iterationDuration,
                $totalRuntime,
                $this->formatBytes($memoryUsage),
                $this->formatBytes($memoryPeakUsage)
            ), 'info', forcePrint: $sleep === 0);

            if ($sleep === 0) {
                return;
            }

            if ($maxTime > 0 && $totalRuntime >= $maxTime) {
                $this->line(sprintf('Stopping: reached max runtime of %d seconds.', $maxTime), 'warning', forcePrint: true);
                return;
            }

            if ($maxMemoryBytes > 0 && $memoryUsage >= $maxMemoryBytes) {
                $this->line(sprintf(
                    'Stopping: memory usage %s exceeded max threshold of %s.',
                    $this->formatBytes($memoryUsage),
                    $this->formatBytes($maxMemoryBytes)
                ), 'warning', forcePrint: true);
                return;
            }

            $this->line(sprintf('Sleep %d seconds...', $sleep));
            sleep($sleep);
        }
    }

    /**
     * @return void
     * @throws \GuzzleHttp\Exception\GuzzleException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     */
    private function processVhosts(): void
    {
        $filter = trim($this->option('filter', ''));
        $hasFilter = '' !== $filter;

        $shouldRemoveOld = !$hasFilter;
        $oldVhostsMap = $shouldRemoveOld ? array_flip($this->internalStorageManager->getVhosts()) : [];

        foreach ($this->getAllApiVhosts() as $vhostApiData) {
            if (is_string($vhostApiData)) {
                $vhostApiData = (array) json_decode($vhostApiData, true);
            }

            $vhostDto = new VhostApiDto($vhostApiData);
            if ($hasFilter && (false === str_contains($vhostDto->getName(), $filter))) {
                continue;
            }

            $isProcessed = $this->processVhost($vhostDto);
            if (false === $isProcessed) {
                continue;
            }

            if ($shouldRemoveOld && array_key_exists($vhostDto->getName(), $oldVhostsMap)) {
                unset($oldVhostsMap[$vhostDto->getName()]);
            }
        }

        if ($shouldRemoveOld) {
            $this->removeOldVhosts(array_keys($oldVhostsMap));
        }
        unset($oldVhostsMap);
    }

    /**
     * @return \Generator
     */
    private function getAllApiVhosts(): Generator|array
    {
        $type = (string) $this->option('type');
        if (self::TYPE_INTERIM === $type) {
            $interimVhosts = $this->internalStorageManager->getInterimVhosts();
            shuffle($interimVhosts);
            return $interimVhosts;
        }

        return $this->vhostsService->getAllVhosts();
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return bool
     * @throws \GuzzleHttp\Exception\GuzzleException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     */
    private function processVhost(VhostApiDto $vhostDto): bool
    {
        if ('' === $vhostDto->getName()) {
            return false;
        }

        $isAddedToIndex = $this->internalStorageManager->indexVhost($vhostDto, $this->groups);

        $this->info(sprintf(
            'Successfully indexed (%s) vhost: "%s". Messages ready: %d. Messages unacknowledged: %d.',
            $isAddedToIndex ? 'added' : 'removed',
            $vhostDto->getName(),
            $vhostDto->getMessagesReady(),
            $vhostDto->getMessagesUnacknowledged()
        ));

        if ($isAddedToIndex) {
            $this->refreshWindowCosts($vhostDto->getName());
        }

        $vhostQueues = $isAddedToIndex ? $this->queueService->getAllVhostQueues($vhostDto) : null;

        $oldVhostQueues = $this->internalStorageManager->getVhostQueues($vhostDto->getName());

        $indexedQueueNames = [];

        if ($vhostQueues && $vhostQueues->isNotEmpty()) {
            foreach ($vhostQueues as $queueApiData) {
                $processedQueue = $this->processVhostQueue($queueApiData);
                if (null === $processedQueue) {
                    continue;
                }

                [$processQueueDto, $isQueueAddedToIndex] = $processedQueue;
                if ($isQueueAddedToIndex) {
                    $indexedQueueNames[] = $processQueueDto->getName();
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

        if ($isAddedToIndex) {
            $this->refreshQueueWindowCosts($vhostDto->getName(), $indexedQueueNames);
        }

        return true;
    }

    /**
     * Recompute the sliding-window processing cost for a vhost so idle vhosts
     * decay towards zero even while no worker is recording time. Only groups
     * scheduled on processing_time are refreshed: the strategy comes from the
     * same groups file the scanner already loads at startup, so - like any other
     * groups change - flipping a group's scheduler_strategy requires a scanner
     * restart to take effect.
     *
     * @param string $vhostName
     * @return void
     */
    private function refreshWindowCosts(string $vhostName): void
    {
        if (empty($this->processingTimeGroups)) {
            return;
        }

        $this->internalStorageManager->refreshWindowCosts(
            $this->processingTimeGroups,
            $vhostName,
            $this->schedulerOptions->getWindow(),
            $this->schedulerOptions->getBucket()
        );
    }

    /**
     * Queue-level counterpart of refreshWindowCosts(): decay each indexed queue's
     * window cost for the processing_time groups so queues that are not being
     * processed slide back toward zero, keeping queue ordering fair. The queue
     * names come from the scan pass itself, so no extra index read is needed,
     * and all refreshes go out as a single pipelined round trip.
     *
     * @param string $vhostName
     * @param array $queueNames
     * @return void
     */
    private function refreshQueueWindowCosts(string $vhostName, array $queueNames): void
    {
        if (empty($queueNames) || empty($this->processingTimeGroups)) {
            return;
        }

        $this->internalStorageManager->refreshQueueWindowCosts(
            $this->processingTimeGroups,
            $vhostName,
            $queueNames,
            $this->schedulerOptions->getWindow(),
            $this->schedulerOptions->getBucket()
        );
    }

    private function formatBytes(int $bytes): string
    {
        return number_format($bytes / (1024 * 1024), 2) . ' MB';
    }

    /**
     * @param array $oldVhosts
     * @return void
     */
    private function removeOldVhosts(array $oldVhosts): void
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
     * @param array $queueApiData
     * @return array{0: QueueApiDto, 1: bool}|null the processed queue and whether it was added to the index
     */
    private function processVhostQueue(array $queueApiData): ?array
    {
        $queueApiDto = new QueueApiDto($queueApiData);
        if ('' === $queueApiDto->getName()) {
            return null;
        }

        $isAddedToIndex = $this->internalStorageManager->indexQueue($queueApiDto, $this->groups);

        $this->info(sprintf(
            'Successfully indexed (%s) queue: "%s". Vhost: %s. Messages ready: %d. Messages unacknowledged: %d.',
            $isAddedToIndex ? 'added' : 'removed',
            $queueApiDto->getName(),
            $queueApiDto->getVhostName(),
            $queueApiDto->getMessagesReady(),
            $queueApiDto->getMessagesUnacknowledged()
        ));

        return [$queueApiDto, $isAddedToIndex];
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

    public function line($string, $style = null, $verbosity = null, $forcePrint = false): void
    {
        if (!$this->silent || ('error' === $style) || $forcePrint) {
            parent::line($string, $style, $verbosity);
        }
    }
}

