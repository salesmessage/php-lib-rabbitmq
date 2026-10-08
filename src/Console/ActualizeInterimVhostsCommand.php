<?php

namespace Salesmessage\LibRabbitMQ\Console;

use Illuminate\Console\Command;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Services\InterimVhosts\InterimVhostsSourceFactory;
use Salesmessage\LibRabbitMQ\Services\InterimVhosts\InterimVhostsSourceInterface;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Throwable;

class ActualizeInterimVhostsCommand extends Command
{
    protected $signature = 'lib-rabbitmq:actualize-interim-vhosts
                            {--connection=rabbitmq_vhosts : The name of the queue connection to work}
                            {--sleep=5 : Number of seconds to sleep}
                            {--max-time=0 : Maximum seconds the command can run before stopping}
                            {--with-output=true : Show output details during iteration}
                            {--max-memory=0 : Maximum memory usage in megabytes before stopping}';

    protected $description = 'Actualize interim vhosts';

    private bool $silent = false;

    private InterimVhostsSourceInterface $vhostsSource;

    /**
     * @param InterimVhostsSourceFactory $vhostsSourceFactory
     * @param InternalStorageManager $internalStorageManager
     * @param LoggerInterface $logger
     */
    public function __construct(
        private InterimVhostsSourceFactory $vhostsSourceFactory,
        private InternalStorageManager $internalStorageManager,
        private LoggerInterface $logger
    ) {
        parent::__construct();
    }

    /**
     * @return void
     */
    public function handle(): void
    {
        $connectionName = (string) $this->option('connection');
        $this->internalStorageManager->setConnection($connectionName);
        $this->vhostsSource = $this->vhostsSourceFactory->make($connectionName);

        $sleep = (int) $this->option('sleep');
        $maxTime = max(0, (int) $this->option('max-time'));
        $this->silent = !filter_var($this->option('with-output'), FILTER_VALIDATE_BOOLEAN);

        $maxMemoryMb = max(0, (int) $this->option('max-memory'));
        $maxMemoryBytes = $maxMemoryMb > 0 ? $maxMemoryMb * 1024 * 1024 : 0;

        $startedAt = microtime(true);

        $this->line(sprintf('Scan started. Connection: %s', $connectionName), 'info');

        while (true) {
            $iterationStartedAt = microtime(true);

            $this->actualizeInterimVhosts();

            $iterationEndedAt = microtime(true);

            $iterationDuration = $iterationEndedAt - $iterationStartedAt;
            $totalRuntime = $iterationEndedAt - $startedAt;
            $memoryUsage = memory_get_usage(true);
            $memoryPeakUsage = memory_get_peak_usage(true);

            $this->line(sprintf(
                'Iteration finished in %.2f seconds (total runtime %.2f seconds). Interim vhosts count %d. Memory usage: %s (peak %s).',
                $iterationDuration,
                $totalRuntime,
                $this->internalStorageManager->getInterimVhostsCount(),
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
     */
    private function actualizeInterimVhosts(): void
    {
        try {
            $vhosts = $this->vhostsSource->getVhosts();
        } catch (Throwable $exception) {
            $this->logger->error('Salesmessage.LibRabbitMQ.Console.ActualizeInterimVhostsCommand.actualizeInterimVhosts.exception', [
                'connection' => (string) $this->option('connection'),
                'message' => $exception->getMessage(),
                'trace' => $exception->getTraceAsString(),
                'error_class' => get_class($exception),
            ]);

            return;
        }

        $oldInterimVhostNames = array_flip(array_keys($this->internalStorageManager->getInterimVhosts()));

        foreach ($vhosts as $vhostDto) {
            $this->internalStorageManager->addInterimVhost($vhostDto);

            unset($oldInterimVhostNames[$vhostDto->getName()]);
        }

        $this->removeOldInterimVhosts(array_keys($oldInterimVhostNames));
    }

    /**
     * @param array $oldInterimVhostNames
     * @return void
     */
    private function removeOldInterimVhosts(array $oldInterimVhostNames): void
    {
        if (empty($oldInterimVhostNames)) {
            return;
        }

        foreach ($oldInterimVhostNames as $oldInterimVhostName) {
            $vhostDto = new VhostApiDto([
                'name' => $oldInterimVhostName,
            ]);

            $this->internalStorageManager->removeInterimVhost($vhostDto);

            $this->warn(sprintf(
                'Removed interim vhost: "%s".',
                $vhostDto->getName()
            ));
        }
    }

    /**
     * @param int $bytes
     * @return string
     */
    private function formatBytes(int $bytes): string
    {
        return number_format($bytes / (1024 * 1024), 2) . ' MB';
    }

    /**
     * @param $string
     * @param $style
     * @param $verbosity
     * @param $forcePrint
     * @return void
     */
    public function line($string, $style = null, $verbosity = null, $forcePrint = false): void
    {
        if (!$this->silent || ($style === 'error') || $forcePrint) {
            parent::line($string, $style, $verbosity);
        }
    }
}

