<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services\QueueExpiry;

use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery\MockInterface;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Services\QueueExpiry\QueueExpiryPolicyOptions;
use Salesmessage\LibRabbitMQ\Services\QueueExpiry\QueueExpiryPolicyService;
use Salesmessage\LibRabbitMQ\Services\QueueExpiry\QueueExpiryPolicySync;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class QueueExpiryPolicySyncTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private MockInterface $vhostsService;

    private MockInterface $policyService;

    private MockInterface $logger;

    private QueueExpiryPolicyOptions $options;

    protected function setUp(): void
    {
        parent::setUp();

        $this->options = QueueExpiryPolicyOptions::fromConfig(['enabled' => true]);

        $this->vhostsService = Mockery::mock(VhostsService::class);
        $this->vhostsService->shouldReceive('getAllVhosts')->with(1, 'name', true)->andReturnUsing(function () {
            yield ['name' => '/'];
            yield ['name' => 'organization_current'];
            yield ['name' => 'organization_missing'];
            yield ['name' => 'organization_different'];
        })->byDefault();

        $this->policyService = Mockery::mock(QueueExpiryPolicyService::class);
        $this->policyService->shouldReceive('getOptions')->andReturnUsing(fn () => $this->options);
        $this->policyService->shouldReceive('getPoliciesByVhost')->andReturn([
            '/' => $this->policy(),
            'organization_current' => $this->policy(),
            'organization_different' => ['definition' => ['expires' => 1]] + $this->policy(),
        ]);

        $this->logger = Mockery::spy(LoggerInterface::class);
    }

    public function test_apply_mode_applies_where_the_policy_is_missing_or_different(): void
    {
        $this->policyService->shouldReceive('apply')->once()->with('organization_missing')->andReturn(true);
        $this->policyService->shouldReceive('apply')->once()->with('organization_different')->andReturn(true);

        $summary = $this->sync()->sync(QueueExpiryPolicySync::MODE_APPLY, [], 0);

        $this->assertSame(
            ['vhosts' => 4, 'current' => 2, 'missing' => 1, 'different' => 1, 'applied' => 2, 'removed' => 0, 'failed' => 0],
            $summary
        );
    }

    public function test_missing_only_mode_leaves_a_different_policy_and_reports_the_drift(): void
    {
        $this->policyService->shouldReceive('apply')->once()->with('organization_missing')->andReturn(true);

        $summary = $this->sync()->sync(QueueExpiryPolicySync::MODE_MISSING_ONLY, [], 0);

        $this->assertSame(1, $summary['applied']);
        $this->logger->shouldHaveReceived('warning')
            ->with('Salesmessage.LibRabbitMQ.Services.QueueExpiryPolicySync.drift', Mockery::subset([
                'vhost_name' => 'organization_different',
            ]))
            ->once();
    }

    public function test_check_mode_writes_nothing_even_while_the_policy_is_disabled(): void
    {
        $this->options = QueueExpiryPolicyOptions::fromConfig(['enabled' => false]);
        $this->policyService->shouldNotReceive('apply');
        $this->policyService->shouldNotReceive('remove');

        $summary = $this->sync()->sync(QueueExpiryPolicySync::MODE_CHECK, [], 0);

        $this->assertSame(['current' => 2, 'missing' => 1, 'different' => 1], array_intersect_key(
            $summary,
            ['current' => 0, 'missing' => 0, 'different' => 0]
        ));
    }

    public function test_remove_mode_removes_every_existing_policy(): void
    {
        $this->options = QueueExpiryPolicyOptions::fromConfig(['enabled' => false]);
        foreach (['/', 'organization_current', 'organization_different'] as $vhostName) {
            $this->policyService->shouldReceive('remove')->once()->with($vhostName)->andReturn(true);
        }

        $summary = $this->sync()->sync(QueueExpiryPolicySync::MODE_REMOVE, [], 0);

        $this->assertSame(3, $summary['removed']);
    }

    public function test_applying_modes_refuse_to_run_while_the_policy_is_disabled(): void
    {
        $this->options = QueueExpiryPolicyOptions::fromConfig(['enabled' => false]);
        $this->policyService->shouldNotReceive('apply');

        $this->expectException(\LogicException::class);

        $this->sync()->sync(QueueExpiryPolicySync::MODE_MISSING_ONLY, [], 0);
    }

    public function test_only_the_given_vhosts_are_synced(): void
    {
        $this->vhostsService->shouldNotReceive('getAllVhosts');
        $this->policyService->shouldReceive('apply')->once()->with('organization_missing')->andReturn(true);

        $summary = $this->sync()->sync(QueueExpiryPolicySync::MODE_APPLY, ['organization_missing', 'organization_current'], 0);

        $this->assertSame(2, $summary['vhosts']);
        $this->assertSame(1, $summary['applied']);
    }

    public function test_failed_writes_are_counted(): void
    {
        $this->policyService->shouldReceive('apply')->andReturn(false);

        $summary = $this->sync()->sync(QueueExpiryPolicySync::MODE_APPLY, [], 0);

        $this->assertSame(0, $summary['applied']);
        $this->assertSame(2, $summary['failed']);
    }

    public function test_an_unknown_mode_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->sync()->sync('everything', [], 0);
    }

    private function sync(): QueueExpiryPolicySync
    {
        return new QueueExpiryPolicySync($this->vhostsService, $this->policyService, $this->logger);
    }

    private function policy(): array
    {
        return ['name' => 'sm-queue-expiry'] + $this->options->toPolicyBody();
    }
}
