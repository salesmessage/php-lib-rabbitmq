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

    private const DEFAULT_IMMEDIATE_INDEXATION_GRACE = 10;

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
    private float $immediateIndexationGrace = self::DEFAULT_IMMEDIATE_INDEXATION_GRACE;
    private float $passStartedAt = 0.0;

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

        $this->immediateIndexationGrace = max(0.0, (float) config(
            'queue.connections.' . $connectionName . '.immediate_indexation_grace',
            self::DEFAULT_IMMEDIATE_INDEXATION_GRACE
        ));

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
        $this->passStartedAt = microtime(true);

        $filter = trim($this->option('filter', ''));
        $hasFilter = '' !== $filter;

        $shouldRemoveOld = !$hasFilter;
        $oldVhostsMap = $shouldRemoveOld ? array_flip($this->internalStorageManager->getVhosts()) : [];

        // a vhost missing from the counts is judged by the oldest counts of the pass
        $oldestCountsReadAt = $this->passStartedAt;

        foreach ($this->getAllApiVhosts() as $vhostApiData) {
            if (is_string($vhostApiData)) {
                $vhostApiData = (array) json_decode($vhostApiData, true);
            }

            $vhostDto = new VhostApiDto($vhostApiData);
            if ($hasFilter && (false === str_contains($vhostDto->getName(), $filter))) {
                continue;
            }

            $oldestCountsReadAt = min($oldestCountsReadAt, $this->getCountsReadAt($vhostDto));

            $isProcessed = $this->processVhost($vhostDto);
            if (false === $isProcessed) {
                continue;
            }

            if ($shouldRemoveOld && array_key_exists($vhostDto->getName(), $oldVhostsMap)) {
                unset($oldVhostsMap[$vhostDto->getName()]);
            }
        }

        if ($shouldRemoveOld) {
            $this->removeOldVhosts(array_keys($oldVhostsMap), $oldestCountsReadAt);
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

        // queues read together with the vhost (Prometheus) need no management API call
        if (null !== $vhostDto->getQueues()) {
            $this->processVhostWithQueues($vhostDto);

            return true;
        }

        $countsReadAt = $this->getCountsReadAt($vhostDto);
        if (!$vhostDto->hasMessages() && $this->isVhostActivatedAfter($vhostDto->getName(), $countsReadAt)) {
            $this->info(sprintf(
                'Kept in index vhost: "%s". A publish indexed it after its counts were read.',
                $vhostDto->getName()
            ));

            return true;
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

        $vhostQueues = null;
        if ($isAddedToIndex) {
            $vhostQueues = $this->queueService->getAllVhostQueues($vhostDto);
            if (null === $vhostQueues) {
                $this->warn(sprintf(
                    'Failed to fetch queues for vhost "%s", indexed queues are kept.',
                    $vhostDto->getName()
                ));

                return true;
            }
        }

        $oldVhostQueues = $this->internalStorageManager->getVhostQueues($vhostDto->getName());

        $indexedQueueNames = [];

        if ($vhostQueues && $vhostQueues->isNotEmpty()) {
            foreach ($vhostQueues as $queueApiData) {
                $processedQueue = $this->processVhostQueue(new QueueApiDto($queueApiData));
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

        $activatedQueueNames = $this->keepActivatedQueues($vhostDto, $oldVhostQueues, $countsReadAt);
        $this->removeOldVhostQueues($vhostDto, array_diff($oldVhostQueues, $activatedQueueNames));

        if ($isAddedToIndex) {
            $this->refreshQueueWindowCosts($vhostDto->getName(), $indexedQueueNames);
        }

        return true;
    }

    /**
     * Index a vhost and its queues from the queues read with it. They are only the ones that may hold
     * messages, so an indexed queue missing from them is idle or gone and is removed. It is kept instead
     * when it exists without counts (e.g. its quorum queue elects a leader), when the read was
     * incomplete (a node was not read) or when a publish indexed it after the counts were read. Their
     * messages are not in the vhost totals, so a vhost with no queue holding messages is kept as it is
     * while it has kept queues.
     *
     * @param VhostApiDto $vhostDto
     * @return void
     */
    private function processVhostWithQueues(VhostApiDto $vhostDto): void
    {
        $queues = (array) $vhostDto->getQueues();

        $keptQueueNames = [];
        $removedQueueNames = [];
        foreach ($this->internalStorageManager->getVhostQueues($vhostDto->getName()) as $indexedQueueName) {
            if (array_key_exists($indexedQueueName, $queues)) {
                if (null === $queues[$indexedQueueName]) {
                    $keptQueueNames[] = $indexedQueueName;
                }

                continue;
            }

            if ($vhostDto->isQueuesComplete()) {
                $removedQueueNames[] = $indexedQueueName;
            } else {
                $keptQueueNames[] = $indexedQueueName;
            }
        }

        $indexedQueueNames = [];
        foreach (array_filter($queues) as $queueDto) {
            $processedQueue = $this->processVhostQueue($queueDto);
            if ((null !== $processedQueue) && $processedQueue[1]) {
                $indexedQueueNames[] = $processedQueue[0]->getName();
            }
        }

        foreach ($keptQueueNames as $keptQueueName) {
            $this->info(sprintf(
                'Kept in index queue: "%s". Vhost: %s. No counts were read for it.',
                $keptQueueName,
                $vhostDto->getName()
            ));
        }

        $activatedQueueNames = $this->keepActivatedQueues(
            $vhostDto,
            $removedQueueNames,
            $this->getCountsReadAt($vhostDto)
        );
        $keptQueueNames = array_merge($keptQueueNames, $activatedQueueNames);

        $this->removeOldVhostQueues($vhostDto, array_diff($removedQueueNames, $activatedQueueNames));

        if (empty($indexedQueueNames) && !empty($keptQueueNames)) {
            $isVhostIndexed = true;

            $this->info(sprintf('Kept in index vhost: "%s". It has kept queues.', $vhostDto->getName()));
        } else {
            $isVhostIndexed = $this->internalStorageManager->indexVhost($vhostDto, $this->groups);

            $this->info(sprintf(
                'Successfully indexed (%s) vhost: "%s". Messages ready: %d. Messages unacknowledged: %d.',
                $isVhostIndexed ? 'added' : 'removed',
                $vhostDto->getName(),
                $vhostDto->getMessagesReady(),
                $vhostDto->getMessagesUnacknowledged()
            ));
        }

        if ($isVhostIndexed) {
            $this->refreshWindowCosts($vhostDto->getName());
            $this->refreshQueueWindowCosts($vhostDto->getName(), array_merge($indexedQueueNames, $keptQueueNames));
        }
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
     * When the counts of the vhost were read: by the actualizer for an interim vhost that carries the
     * time, otherwise not before the pass started.
     *
     * @param VhostApiDto $vhostDto
     * @return float
     */
    private function getCountsReadAt(VhostApiDto $vhostDto): float
    {
        return $vhostDto->getActualizedAt() ?? $this->passStartedAt;
    }

    /**
     * Whether a publish indexed the entry too late for the counts read at $countsReadAt to be trusted
     * about it: RabbitMQ updates queue counts every few seconds, so counts read up to the grace period
     * after the publish may not show its message yet.
     *
     * @param float|null $activatedAt
     * @param float $countsReadAt
     * @return bool
     */
    private function isActivatedAfter(?float $activatedAt, float $countsReadAt): bool
    {
        return (null !== $activatedAt) && ($activatedAt >= $countsReadAt - $this->immediateIndexationGrace);
    }

    /**
     * @param string $vhostName
     * @param float $countsReadAt
     * @return bool
     */
    private function isVhostActivatedAfter(string $vhostName, float $countsReadAt): bool
    {
        $activatedAt = $this->internalStorageManager->getVhostActivatedAt(new VhostApiDto([
            'name' => $vhostName,
        ]));

        return $this->isActivatedAfter($activatedAt, $countsReadAt);
    }

    /**
     * The queues of $queueNames a publish indexed after the counts were read. They stay indexed.
     *
     * @param VhostApiDto $vhostDto
     * @param array $queueNames
     * @param float $countsReadAt
     * @return array
     */
    private function keepActivatedQueues(VhostApiDto $vhostDto, array $queueNames, float $countsReadAt): array
    {
        $activatedQueueNames = [];
        foreach ($queueNames as $queueName) {
            $activatedAt = $this->internalStorageManager->getQueueActivatedAt(new QueueApiDto([
                'name' => $queueName,
                'vhost' => $vhostDto->getName(),
            ]));
            if (!$this->isActivatedAfter($activatedAt, $countsReadAt)) {
                continue;
            }

            $activatedQueueNames[] = $queueName;

            $this->info(sprintf(
                'Kept in index queue: "%s". Vhost: %s. A publish indexed it after the counts were read.',
                $queueName,
                $vhostDto->getName()
            ));
        }

        return $activatedQueueNames;
    }

    /**
     * @param array $oldVhosts
     * @param float $countsReadAt
     * @return void
     */
    private function removeOldVhosts(array $oldVhosts, float $countsReadAt): void
    {
        if (empty($oldVhosts)) {
            return;
        }

        foreach ($oldVhosts as $oldVhostName) {
            if ($this->isVhostActivatedAfter($oldVhostName, $countsReadAt)) {
                $this->info(sprintf(
                    'Kept in index vhost: "%s". A publish indexed it after the counts were read.',
                    $oldVhostName
                ));

                continue;
            }

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
     * @param QueueApiDto $queueApiDto
     * @return array{0: QueueApiDto, 1: bool}|null the processed queue and whether it was added to the index
     */
    private function processVhostQueue(QueueApiDto $queueApiDto): ?array
    {
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

