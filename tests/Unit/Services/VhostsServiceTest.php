<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services;

use Mockery;
use Mockery\MockInterface;
use Orchestra\Testbench\TestCase;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException;
use Salesmessage\LibRabbitMQ\Services\Api\RabbitApiClient;
use Salesmessage\LibRabbitMQ\Services\QueueExpiry\QueueExpiryPolicyService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class VhostsServiceTest extends TestCase
{
    private MockInterface $apiClient;

    private MockInterface $policyService;

    protected function setUp(): void
    {
        parent::setUp();

        $this->apiClient = Mockery::mock(RabbitApiClient::class);
        $this->apiClient->shouldReceive('setConnectionConfig');
        $this->apiClient->shouldReceive('getUsername')->andReturn('app');

        $this->policyService = Mockery::mock(QueueExpiryPolicyService::class);
        $this->policyService->shouldReceive('setConnection')->andReturnSelf()->byDefault();
    }

    public function test_a_created_vhost_gets_the_queue_expiry_policy(): void
    {
        $this->apiClient->shouldReceive('request')->with('PUT', '/api/vhosts/organization_1', [], Mockery::any())->once();
        $this->apiClient->shouldReceive('request')->with('PUT', '/api/permissions/organization_1/app', [], Mockery::any())->once();
        $this->policyService->shouldReceive('applyIfEnabled')->once()->with('organization_1')->andReturn(true);

        $this->assertTrue($this->service()->createVhost('organization_1', 'Vhost for organization ID: 1'));
    }

    public function test_a_failed_policy_apply_does_not_fail_the_vhost_creation(): void
    {
        $this->apiClient->shouldReceive('request')->twice();
        $this->policyService->shouldReceive('applyIfEnabled')->once()->andReturn(false);

        $this->assertTrue($this->service()->createVhost('organization_1', 'Vhost for organization ID: 1'));
    }

    public function test_no_policy_is_applied_when_the_permissions_could_not_be_set(): void
    {
        $this->apiClient->shouldReceive('request')->with('PUT', '/api/vhosts/organization_1', [], Mockery::any())->once();
        $this->apiClient->shouldReceive('request')->with('PUT', '/api/permissions/organization_1/app', [], Mockery::any())
            ->andThrow(new RabbitApiClientException('forbidden'));
        $this->policyService->shouldNotReceive('applyIfEnabled');

        $this->assertFalse($this->service()->createVhost('organization_1', 'Vhost for organization ID: 1'));
    }

    public function test_switching_the_connection_switches_the_policy_service_too(): void
    {
        $service = $this->service();

        $this->policyService->shouldReceive('setConnection')->once()->with('rabbitmq_cluster')->andReturnSelf();

        $service->setConnection('rabbitmq_cluster');
    }

    private function service(): VhostsService
    {
        return new VhostsService($this->apiClient, Mockery::spy(LoggerInterface::class), $this->policyService);
    }
}
