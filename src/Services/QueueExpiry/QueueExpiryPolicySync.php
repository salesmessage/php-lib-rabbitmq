<?php

namespace Salesmessage\LibRabbitMQ\Services\QueueExpiry;

use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class QueueExpiryPolicySync
{
    public const MODE_APPLY = 'apply';

    public const MODE_MISSING_ONLY = 'missing-only';

    public const MODE_CHECK = 'check';

    public const MODE_REMOVE = 'remove';

    private const STATE_CURRENT = 'current';

    private const STATE_MISSING = 'missing';

    private const STATE_DIFFERENT = 'different';

    private const ACTION_APPLY = 'apply';

    private const ACTION_REMOVE = 'remove';

    public function __construct(
        private VhostsService $vhostsService,
        private QueueExpiryPolicyService $policyService,
        private LoggerInterface $logger
    ) {}

    public function setConnection(string $connectionName): self
    {
        $this->vhostsService->setConnection($connectionName);
        $this->policyService->setConnection($connectionName);

        return $this;
    }

    /**
     * @param  list<string>  $onlyVhosts  empty means every vhost of the connection
     * @param  float|null  $ratePerSecond  writes per second, null takes it from the config, 0 means no limit
     * @return array{vhosts: int, current: int, missing: int, different: int, applied: int, removed: int, failed: int}
     *
     * @throws \InvalidArgumentException when the mode is unknown
     * @throws \LogicException when an applying mode runs while the policy is disabled
     * @throws \GuzzleHttp\Exception\GuzzleException
     * @throws \Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException
     */
    public function sync(string $mode, array $onlyVhosts = [], float $ratePerSecond = null): array
    {
        if (! in_array($mode, [self::MODE_APPLY, self::MODE_MISSING_ONLY, self::MODE_CHECK, self::MODE_REMOVE], true)) {
            throw new \InvalidArgumentException(sprintf('Unknown queue expiry policy sync mode "%s".', $mode));
        }

        $options = $this->policyService->getOptions();
        if (in_array($mode, [self::MODE_APPLY, self::MODE_MISSING_ONLY], true) && ($options->isEnabled() === false)) {
            throw new \LogicException(sprintf('Queue expiry policy "%s" is disabled.', $options->getName()));
        }

        $ratePerSecond ??= $options->getApplyRatePerSecond();
        $policies = $this->policyService->getPoliciesByVhost();

        $summary = ['vhosts' => 0, 'current' => 0, 'missing' => 0, 'different' => 0, 'applied' => 0, 'removed' => 0, 'failed' => 0];

        foreach ($this->vhostNames($onlyVhosts) as $vhostName) {
            $summary['vhosts']++;

            $policy = $policies[$vhostName] ?? null;
            $state = $this->state($policy);
            $summary[$state]++;

            if ($state === self::STATE_DIFFERENT) {
                $this->logger->warning('Salesmessage.LibRabbitMQ.Services.QueueExpiryPolicySync.drift', [
                    'vhost_name' => $vhostName,
                    'policy' => $policy,
                    'expected' => $options->toPolicyBody(),
                ]);
            }

            $action = $this->action($mode, $state);
            if ($action === null) {
                continue;
            }

            if ($action === self::ACTION_APPLY) {
                $isSuccess = $this->policyService->apply($vhostName);
                $successKey = 'applied';
            } else {
                $isSuccess = $this->policyService->remove($vhostName);
                $successKey = 'removed';
            }
            $summary[$isSuccess ? $successKey : 'failed']++;

            $this->pause($ratePerSecond);
        }

        return $summary;
    }

    protected function pause(float $ratePerSecond): void
    {
        if ($ratePerSecond > 0) {
            usleep((int) (1000000 / $ratePerSecond));
        }
    }

    /**
     * @param  list<string>  $onlyVhosts
     * @return iterable<string>
     */
    private function vhostNames(array $onlyVhosts): iterable
    {
        if (! empty($onlyVhosts)) {
            yield from $onlyVhosts;

            return;
        }

        foreach ($this->vhostsService->getAllVhosts(1, 'name', true) as $vhost) {
            yield (string) $vhost['name'];
        }
    }

    private function state(?array $policy): string
    {
        if ($policy === null) {
            return self::STATE_MISSING;
        }

        return $this->policyService->getOptions()->matches($policy) ? self::STATE_CURRENT : self::STATE_DIFFERENT;
    }

    private function action(string $mode, string $state): ?string
    {
        return match ($mode) {
            self::MODE_APPLY => ($state !== self::STATE_CURRENT) ? self::ACTION_APPLY : null,
            self::MODE_MISSING_ONLY => ($state === self::STATE_MISSING) ? self::ACTION_APPLY : null,
            self::MODE_REMOVE => ($state !== self::STATE_MISSING) ? self::ACTION_REMOVE : null,
            default => null,
        };
    }
}
