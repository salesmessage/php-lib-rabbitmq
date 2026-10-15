<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services\Deduplication;

use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\Attributes\DataProvider;
use Psr\Log\NullLogger;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationStore;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;
use Salesmessage\LibRabbitMQ\Tests\TestCase;

class DeduplicationServiceLockRequeueConfirmTimeoutTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    protected function setUp(): void
    {
        parent::setUp();

        config()->set('queue.drivers.rabbitmq_vhosts.deduplication.transport', [
            'enabled' => true,
            'action_on_lock' => 'requeue',
            'lock_requeue_max_attempts' => 1,
        ]);
        config()->set('queue.connections.rabbitmq_cluster.options.queue.publisher_confirm_timeout', 10);
        config()->set('queue.connections.rabbitmq_vhosts.options.queue.publisher_confirm_timeout', 30);
    }

    public static function connectionsWithConfirmTimeouts(): array
    {
        return [
            'vhost connection of a configured connection' => ['rabbitmq_cluster:vhost:organization_1', 10.0],
            'configured connection without a vhost' => ['rabbitmq_cluster', 10.0],
            'connection without a configured timeout' => ['rabbitmq', 5.0],
            'no connection name' => [null, 5.0],
        ];
    }

    #[DataProvider('connectionsWithConfirmTimeouts')]
    public function test_locked_message_republish_waits_for_the_confirm_timeout_of_its_connection(
        ?string $connectionName,
        float $expectedTimeout
    ): void {
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('queue_declare')->once();
        $channel->shouldReceive('confirm_select')->once();
        $channel->shouldReceive('set_nack_handler')->once();
        $channel->shouldReceive('set_return_listener')->once();
        $channel->shouldReceive('basic_publish')->once();
        $channel->shouldReceive('wait_for_pending_acks_returns')->once()->with($expectedTimeout);
        $channel->shouldReceive('basic_ack')->once()->with(4, false);

        $handlerRan = $this->makeService()->decorateWithDeduplication(
            fn () => $this->fail('A locked message must not be handled'),
            $this->makeMessage($channel),
            'default-quorum',
            $connectionName
        );

        $this->assertFalse($handlerRan);
    }

    private function makeService(): DeduplicationService
    {
        $store = Mockery::mock(DeduplicationStore::class);
        $store->shouldReceive('get')->andReturn(DeduplicationService::IN_PROGRESS);

        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldNotReceive('log');

        return new DeduplicationService($store, new NullLogger, new FailedJobWriter($failer, new NullLogger));
    }

    private function makeMessage(AMQPChannel $channel): AMQPMessage
    {
        $message = new AMQPMessage('{"uuid":"0b6a0d5c-2a8e-4f6e-9a39-3f8d1f7b9a10"}', [
            'message_id' => 'message-1',
        ]);
        $message->setChannel($channel);
        $message->setDeliveryInfo(4, false, '', 'default-quorum');

        return $message;
    }
}
