<?php

namespace Salesmessage\LibRabbitMQ\Console;

use Illuminate\Console\Command;
use Salesmessage\LibRabbitMQ\Services\QueueExpiry\QueueExpiryPolicySync;

class QueueExpiryPolicySyncCommand extends Command
{
    protected $signature = 'lib-rabbitmq:queue-expiry-policy:sync
                            {--connection=rabbitmq_vhosts : The name of the queue connection to work}
                            {--vhost=* : Only these vhosts (all vhosts of the connection when omitted)}
                            {--check : Only report vhosts where the policy is missing or different}
                            {--missing-only : Apply the policy only where it is missing, report the different ones}
                            {--remove : Delete the policy from every vhost}
                            {--rate= : Policy writes per second (the configured rate when omitted, 0 = no limit)}';

    protected $description = 'Sync the queue expiry operator policy to vhosts';

    public function __construct(
        private QueueExpiryPolicySync $queueExpiryPolicySync
    ) {
        parent::__construct();
    }

    public function handle(): int
    {
        $modes = array_keys(array_filter([
            QueueExpiryPolicySync::MODE_CHECK => (bool) $this->option('check'),
            QueueExpiryPolicySync::MODE_MISSING_ONLY => (bool) $this->option('missing-only'),
            QueueExpiryPolicySync::MODE_REMOVE => (bool) $this->option('remove'),
        ]));
        if (count($modes) > 1) {
            $this->error('Use only one of --check, --missing-only and --remove.');

            return self::FAILURE;
        }

        $mode = $modes[0] ?? QueueExpiryPolicySync::MODE_APPLY;
        $rate = ($this->option('rate') !== null) ? (float) $this->option('rate') : null;

        try {
            $summary = $this->queueExpiryPolicySync
                ->setConnection((string) $this->option('connection'))
                ->sync($mode, array_values(array_filter((array) $this->option('vhost'))), $rate);
        } catch (\LogicException $exception) {
            $this->warn($exception->getMessage());

            return self::SUCCESS;
        }

        $this->info(sprintf('Queue expiry policy sync finished. Mode: %s.', $mode));
        $this->table(array_keys($summary), [array_values($summary)]);

        return ($summary['failed'] > 0) ? self::FAILURE : self::SUCCESS;
    }
}
