<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services\QueueExpiry;

use Mockery;
use Mockery\MockInterface;
use Orchestra\Testbench\TestCase;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Salesmessage\LibRabbitMQ\Services\QueueExpiry\QueueExpiryPolicyService;

class QueueExpiryPolicyServiceTest extends TestCase
{
    private const BODY = [
        'pattern' => '^(?!.*failed)(?!.*dlq).+$',
        'definition' => ['expires' => 259200000],
        'priority' => 0,
        'apply-to' => 'queues',
    ];

    private MockInterface $apiClient;

    private MockInterface $logger;

    protected function setUp(): void
    {
        parent::setUp();

        $this->apiClient = Mockery::mock(RabbitApiClient::class);
        $this->apiClient->shouldReceive('setConnectionConfig')->byDefault();
        $this->logger = Mockery::spy(LoggerInterface::class);
    }

    public function test_apply_puts_the_operator_policy_into_the_vhost(): void
    {
        $this->apiClient->shouldReceive('request')->once()
            ->with('PUT', '/api/operator-policies/organization_1/sm-queue-expiry', [], self::BODY)
            ->andReturn([]);

        $this->assertTrue($this->service()->apply('organization_1'));
    }

    public function test_the_root_vhost_is_url_encoded(): void
    {
        $this->apiClient->shouldReceive('request')->once()
            ->with('PUT', '/api/operator-policies/%2F/sm-queue-expiry', [], self::BODY)
            ->andReturn([]);

        $this->assertTrue($this->service()->apply('/'));
    }

    public function test_a_failed_apply_is_logged_and_reported_as_false(): void
    {
        $this->apiClient->shouldReceive('request')->andThrow(new RabbitApiClientException('boom'));

        $this->assertFalse($this->service()->apply('organization_1'));
        $this->logger->shouldHaveReceived('error')
            ->with('Salesmessage.LibRabbitMQ.Services.QueueExpiryPolicyService.apply.exception', Mockery::subset([
                'vhost_name' => 'organization_1',
                'message' => 'boom',
            ]))
            ->once();
    }

    public function test_apply_if_enabled_does_nothing_while_the_policy_is_disabled(): void
    {
        $this->apiClient->shouldNotReceive('request');

        $this->assertFalse($this->service()->applyIfEnabled('organization_1'));
    }

    public function test_apply_if_enabled_applies_when_the_policy_is_enabled(): void
    {
        $this->app['config']->set('queue.drivers.rabbitmq_vhosts.queue_expiry.enabled', true);
        $this->apiClient->shouldReceive('request')->once()
            ->with('PUT', '/api/operator-policies/organization_1/sm-queue-expiry', [], self::BODY)
            ->andReturn([]);

        $this->assertTrue($this->service()->applyIfEnabled('organization_1'));
    }

    public function test_an_invalid_config_neither_breaks_construction_nor_vhost_creation(): void
    {
        $this->app['config']->set('queue.drivers.rabbitmq_vhosts.queue_expiry', ['enabled' => true, 'expires_ms' => -1]);
        $this->apiClient->shouldNotReceive('request');

        $this->assertFalse($this->service()->applyIfEnabled('organization_1'));
        $this->logger->shouldHaveReceived('error')
            ->with('Salesmessage.LibRabbitMQ.Services.QueueExpiryPolicyService.applyIfEnabled.invalid_config', Mockery::any())
            ->once();
    }

    public function test_remove_deletes_the_operator_policy_from_the_vhost(): void
    {
        $this->apiClient->shouldReceive('request')->once()
            ->with('DELETE', '/api/operator-policies/organization_1/sm-queue-expiry')
            ->andReturn([]);

        $this->assertTrue($this->service()->remove('organization_1'));
    }

    public function test_policies_are_keyed_by_vhost_and_other_policies_are_ignored(): void
    {
        $ours = ['vhost' => 'organization_1', 'name' => 'sm-queue-expiry'] + self::BODY;
        $this->apiClient->shouldReceive('request')->once()->with('GET', '/api/operator-policies')->andReturn([
            $ours,
            ['vhost' => 'organization_2', 'name' => 'someone-else'] + self::BODY,
        ]);

        $this->assertSame(['organization_1' => $ours], $this->service()->getPoliciesByVhost());
    }

    public function test_set_connection_uses_the_connection_config(): void
    {
        $this->app['config']->set('queue.connections.rabbitmq_cluster', ['hosts' => [['api_host' => 'cluster']]]);
        $this->apiClient->shouldReceive('setConnectionConfig')->once()->with(['hosts' => [['api_host' => 'cluster']]]);

        $this->service()->setConnection('rabbitmq_cluster');
    }

    private function service(): QueueExpiryPolicyService
    {
        return new QueueExpiryPolicyService($this->apiClient, $this->logger);
    }
}
