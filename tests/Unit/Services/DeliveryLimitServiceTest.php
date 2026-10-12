<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services;

use Illuminate\Config\Repository;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use RuntimeException;
use Salesmessage\LibRabbitMQ\Exceptions\DeliveryLimitReachedException;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;

class DeliveryLimitServiceTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const CONNECTION = 'rabbitmq_cluster:vhost:organization_1';

    private const PAYLOAD = '{"uuid":"0b6a0d5c-2a8e-4f6e-9a39-3f8d1f7b9a10"}';

    public function test_message_below_the_limit_is_allowed(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldNotReceive('log');
        $channel = Mockery::mock(AMQPChannel::class);

        $service = $this->makeService($failer);

        $this->assertTrue($service->isAllowed($this->makeMessage($channel, 2), self::CONNECTION, 'default-quorum'));
    }

    public function test_message_at_the_limit_is_written_to_failed_jobs_and_rejected(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()
            ->with(self::CONNECTION, 'default-quorum', self::PAYLOAD, Mockery::type(DeliveryLimitReachedException::class))
            ->andReturn(1);
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_reject')->once()->with(9, false);

        $service = $this->makeService($failer);

        $this->assertFalse($service->isAllowed($this->makeMessage($channel, 3), self::CONNECTION, 'default-quorum'));
    }

    public function test_ack_action_writes_the_row_before_acking(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->andReturn(1);
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_ack')->once()->with(9, false);

        $service = $this->makeService($failer, 'ack');

        $this->assertFalse($service->isAllowed($this->makeMessage($channel, 3), self::CONNECTION, 'default-quorum'));
    }

    public function test_message_is_requeued_when_the_row_is_not_written(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->andThrow(new RuntimeException('database is down'));
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_nack')->once()->with(9, false, true);
        $channel->shouldNotReceive('basic_reject');

        $service = $this->makeService($failer);

        $this->assertFalse($service->isAllowed($this->makeMessage($channel, 3), self::CONNECTION, 'default-quorum'));
    }

    public function test_queue_defaults_to_the_routing_key(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->with(self::CONNECTION, 'routed-queue', Mockery::any(), Mockery::any())->andReturn(1);
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_reject')->once();

        $service = $this->makeService($failer);

        $this->assertFalse($service->isAllowed($this->makeMessage($channel, 3), self::CONNECTION));
    }

    private function makeService(FailedJobProviderInterface $failer, string $action = 'reject'): DeliveryLimitService
    {
        $config = new Repository([
            'queue' => [
                'connections' => [
                    'rabbitmq_cluster' => [
                        'options' => [
                            'quorum' => [
                                'delivery_limit' => 3,
                                'on_limit_action' => $action,
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        return new DeliveryLimitService($config, new NullLogger, new FailedJobWriter($failer, new NullLogger));
    }

    private function makeMessage(AMQPChannel $channel, int $deliveryCount): AMQPMessage
    {
        $message = new AMQPMessage(self::PAYLOAD, [
            'application_headers' => new AMQPTable(['x-delivery-count' => $deliveryCount]),
        ]);
        $message->setChannel($channel);
        $message->setDeliveryInfo(9, true, '', 'routed-queue');

        return $message;
    }
}
