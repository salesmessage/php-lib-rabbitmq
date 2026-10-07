<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services\QueueExpiry;

use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Services\QueueExpiry\QueueExpiryPolicyOptions;

class QueueExpiryPolicyOptionsTest extends TestCase
{
    public function test_defaults_are_disabled_three_days_and_skip_failed_and_dlq_queues(): void
    {
        $options = QueueExpiryPolicyOptions::fromConfig([]);

        $this->assertFalse($options->isEnabled());
        $this->assertSame('sm-queue-expiry', $options->getName());
        $this->assertSame(2.0, $options->getApplyRatePerSecond());
        $this->assertSame([
            'pattern' => '^(?!.*failed)(?!.*dlq).+$',
            'definition' => ['expires' => 259200000],
            'priority' => 0,
            'apply-to' => 'queues',
        ], $options->toPolicyBody());
    }

    public function test_values_are_read_from_config(): void
    {
        $options = QueueExpiryPolicyOptions::fromConfig([
            'enabled' => 'true',
            'name' => 'custom-expiry',
            'pattern' => '^orders',
            'expires_ms' => '604800000',
            'priority' => '5',
            'apply_rate_per_second' => '0.5',
        ]);

        $this->assertTrue($options->isEnabled());
        $this->assertSame('custom-expiry', $options->getName());
        $this->assertSame(604800000, $options->getExpiresMs());
        $this->assertSame(0.5, $options->getApplyRatePerSecond());
        $this->assertSame([
            'pattern' => '^orders',
            'definition' => ['expires' => 604800000],
            'priority' => 5,
            'apply-to' => 'queues',
        ], $options->toPolicyBody());
    }

    public function test_empty_env_values_fall_back_to_the_defaults(): void
    {
        $options = QueueExpiryPolicyOptions::fromConfig([
            'name' => '',
            'pattern' => '',
            'expires_ms' => '',
            'priority' => '',
            'apply_rate_per_second' => '',
        ]);

        $this->assertSame('sm-queue-expiry', $options->getName());
        $this->assertSame(259200000, $options->getExpiresMs());
        $this->assertSame(2.0, $options->getApplyRatePerSecond());
        $this->assertSame('^(?!.*failed)(?!.*dlq).+$', $options->toPolicyBody()['pattern']);
    }

    public function test_non_positive_expiry_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        QueueExpiryPolicyOptions::fromConfig(['expires_ms' => 0]);
    }

    public function test_a_policy_matches_only_when_pattern_definition_priority_and_target_are_equal(): void
    {
        $options = QueueExpiryPolicyOptions::fromConfig([]);
        $policy = [
            'vhost' => 'organization_1',
            'name' => 'sm-queue-expiry',
            'pattern' => '^(?!.*failed)(?!.*dlq).+$',
            'apply-to' => 'queues',
            'definition' => ['expires' => 259200000],
            'priority' => 0,
        ];

        $this->assertTrue($options->matches($policy));
        $this->assertFalse($options->matches(['definition' => ['expires' => 1]] + $policy));
        $this->assertFalse($options->matches(['definition' => ['expires' => 259200000, 'max-length' => 10]] + $policy));
        $this->assertFalse($options->matches(['pattern' => '.*'] + $policy));
        $this->assertFalse($options->matches(['priority' => 1] + $policy));
        $this->assertFalse($options->matches(['apply-to' => 'all'] + $policy));
    }
}
