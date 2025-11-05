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
                            {--sleep=10 : Number of seconds to sleep}';

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

        while (true) {
            $this->processVhosts();

            if ($sleep === 0) {
                return;
            }

            $this->line(sprintf('Sleep %d seconds...', $sleep));
            sleep($sleep);
        }
    }

    private function processVhosts(): void
    {
        $oldVhosts = [];
        foreach ($this->internalStorageManager->getVhosts() as $idx => $vhost) {
            $oldVhosts[$vhost] = $idx;
        }

        foreach ($this->vhostsService->getAllVhosts() as $vhost) {
            $vhostDto = $this->processVhost($vhost);
            if (null === $vhostDto) {
                continue;
            }

            $oldVhostIndex = $oldVhosts[$vhostDto->getName()] ?? null;
            if ($oldVhostIndex !== null) {
                unset($oldVhosts[$oldVhostIndex]);
            }
        }

        $this->removeOldsVhosts($oldVhosts);
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
