<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services\Deduplication;

use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Psr\Log\NullLogger;
use RuntimeException;
use Salesmessage\LibRabbitMQ\Exceptions\LockRequeueLimitReachedException;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationStore;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;
use Salesmessage\LibRabbitMQ\Tests\TestCase;

class DeduplicationServiceLockRequeueLimitTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const CONNECTION = 'rabbitmq_cluster:vhost:organization_1';

    private const PAYLOAD = '{"uuid":"0b6a0d5c-2a8e-4f6e-9a39-3f8d1f7b9a10"}';

    protected function setUp(): void
    {
        parent::setUp();

        config()->set('queue.drivers.rabbitmq_vhosts.deduplication.transport', [
            'enabled' => true,
            'action_on_lock' => 'requeue',
            'lock_requeue_max_attempts' => 1,
        ]);
    }

    public function test_locked_message_over_the_requeue_limit_is_written_to_failed_jobs_and_rejected(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()
            ->with(self::CONNECTION, 'default-quorum', self::PAYLOAD, Mockery::type(LockRequeueLimitReachedException::class))
            ->andReturn(1);
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_reject')->once()->with(4, false);
        $channel->shouldNotReceive('basic_publish');

        $handlerRan = $this->makeService($failer)->decorateWithDeduplication(
            fn () => $this->fail('A locked message must not be handled'),
            $this->makeMessage($channel),
            'default-quorum',
            self::CONNECTION
        );

        $this->assertFalse($handlerRan);
    }

    public function test_locked_message_is_requeued_when_the_row_is_not_written(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->andThrow(new RuntimeException('database is down'));
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_nack')->once()->with(4, false, true);
        $channel->shouldNotReceive('basic_reject');

        $handlerRan = $this->makeService($failer)->decorateWithDeduplication(
            fn () => $this->fail('A locked message must not be handled'),
            $this->makeMessage($channel),
            'default-quorum',
            self::CONNECTION
        );

        $this->assertFalse($handlerRan);
    }

    private function makeService(FailedJobProviderInterface $failer): DeduplicationService
    {
        $store = Mockery::mock(DeduplicationStore::class);
        $store->shouldReceive('get')->andReturn(DeduplicationService::IN_PROGRESS);

        return new DeduplicationService($store, new NullLogger, new FailedJobWriter($failer, new NullLogger));
    }

    private function makeMessage(AMQPChannel $channel): AMQPMessage
    {
        $message = new AMQPMessage(self::PAYLOAD, [
            'message_id' => 'message-1',
            'application_headers' => new AMQPTable(['x-dup-lock-requeue-count' => 1]),
        ]);
        $message->setChannel($channel);
        $message->setDeliveryInfo(4, true, '', 'default-quorum');

        return $message;
    }
}
