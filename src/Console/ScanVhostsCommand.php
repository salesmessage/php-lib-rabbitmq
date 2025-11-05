<?php

namespace Salesmessage\LibRabbitMQ\Console;

use Illuminate\Console\Command;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\QueueService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;

class ScanVhostsCommand extends Command
{
    protected $signature = 'lib-rabbitmq:scan-vhosts
                            {--sleep=10 : Number of seconds to sleep}
                            {--max-time=0 : Maximum seconds the command can run before stopping}
                            {--max-memory=0 : Maximum memory usage in megabytes before stopping}';

    protected $description = 'Scan and index vhosts';

    private array $groups;

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

        $this->groups = $this->groupsService->getAllGroupsNames();
    }

    public function handle(): void
    {
        $sleep = (int) $this->option('sleep');
        $maxTime = max(0, (int) $this->option('max-time'));
        $maxMemoryMb = max(0, (int) $this->option('max-memory'));
        $maxMemoryBytes = $maxMemoryMb > 0 ? $maxMemoryMb * 1024 * 1024 : 0;

        $startedAt = microtime(true);

        while (true) {
            $iterationStartedAt = microtime(true);

            $this->processVhosts();

            $iterationDuration = microtime(true) - $iterationStartedAt;
            $totalRuntime = microtime(true) - $startedAt;
            $memoryUsage = memory_get_usage(true);
            $memoryPeakUsage = memory_get_peak_usage(true);

            $this->line(sprintf(
                'Iteration finished in %.2f seconds (total runtime %.2f seconds). Memory usage: %s (peak %s).',
                $iterationDuration,
                $totalRuntime,
                $this->formatBytes($memoryUsage),
                $this->formatBytes($memoryPeakUsage)
            ));

            if ($sleep === 0) {
                return;
            }

            if ($maxTime > 0 && $totalRuntime >= $maxTime) {
                $this->warn(sprintf('Stopping: reached max runtime of %d seconds.', $maxTime));
                return;
            }

            if ($maxMemoryBytes > 0 && $memoryUsage >= $maxMemoryBytes) {
                $this->warn(sprintf(
                    'Stopping: memory usage %s exceeded max threshold of %s.',
                    $this->formatBytes($memoryUsage),
                    $this->formatBytes($maxMemoryBytes)
                ));
                return;
            }

            $this->line(sprintf('Sleep %d seconds...', $sleep));
            sleep($sleep);
        }
    }

    private function processVhosts(): void
    {
        $oldVhostsMap = array_flip($this->internalStorageManager->getVhosts());

        foreach ($this->vhostsService->getAllVhosts() as $vhost) {
            $vhostDto = $this->processVhost($vhost);
            if (null === $vhostDto) {
                continue;
            }

            unset($oldVhostsMap[$vhostDto->getName()]);
        }

        $this->removeOldsVhosts(array_keys($oldVhostsMap));
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

        $indexedSuccessfully = $this->internalStorageManager->indexVhost($vhostDto, $this->groups);
        if (!$indexedSuccessfully) {
            $this->warn(sprintf(
                'Skip indexation vhost: "%s". Messages ready: %d. Messages unacknowledged: %d.',
                $vhostDto->getName(),
                $vhostDto->getMessagesReady(),
                $vhostDto->getMessagesUnacknowledged()
            ));

            return null;
        }

        $this->info(sprintf(
            'Successfully indexed vhost: "%s". Messages ready: %d. Messages unacknowledged: %d.',
            $vhostDto->getName(),
            $vhostDto->getMessagesReady(),
            $vhostDto->getMessagesUnacknowledged()
        ));

        $vhostQueues = $this->queueService->getAllVhostQueues($vhostDto);

        $oldVhostQueues = $this->internalStorageManager->getVhostQueues($vhostDto->getName());

        if ($vhostQueues->isNotEmpty()) {
            foreach ($vhostQueues as $queueApiData) {
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

    private function formatBytes(int $bytes): string
    {
        return number_format($bytes / (1024 * 1024), 2) . ' MB';
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
     * @param array $queueApiData
     * @return void
     */
    private function processVhostQueue(array $queueApiData): ?QueueApiDto
    {
        $queueApiDto = new QueueApiDto($queueApiData);
        if ('' === $queueApiDto->getName()) {
            return null;
        }

        $indexedSuccessfully = $this->internalStorageManager->indexQueue($queueApiDto, $this->groups);
        if (!$indexedSuccessfully) {
            $this->warn(sprintf(
                'Skip indexation queue: "%s". Vhost: %s. Messages ready: %d. Messages unacknowledged: %d.',
                $queueApiDto->getName(),
                $queueApiDto->getVhostName(),
                $queueApiDto->getMessagesReady(),
                $queueApiDto->getMessagesUnacknowledged()
            ));

            return null;
        }

        $this->info(sprintf(
            'Successfully indexed queue: "%s". Vhost: %s. Messages ready: %d. Messages unacknowledged: %d.',
            $queueApiDto->getName(),
            $queueApiDto->getVhostName(),
            $queueApiDto->getMessagesReady(),
            $queueApiDto->getMessagesUnacknowledged()
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
