<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Services;

use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use RuntimeException;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;

class FailedJobWriterTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const CONNECTION = 'rabbitmq_cluster:vhost:organization_1';

    private const PAYLOAD = '{"uuid":"0b6a0d5c-2a8e-4f6e-9a39-3f8d1f7b9a10","data":{"commandName":"App\\\\Jobs\\\\Job"}}';

    public function test_write_stores_the_row_through_the_failer(): void
    {
        $exception = new RuntimeException('boom');
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->with(self::CONNECTION, 'default-quorum', self::PAYLOAD, $exception)->andReturn(7);

        $writer = new FailedJobWriter($failer, new NullLogger);

        $this->assertTrue($writer->write(self::CONNECTION, 'default-quorum', self::PAYLOAD, $exception));
    }

    public function test_write_returns_false_when_the_failer_throws(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->andThrow(new RuntimeException('database is down'));

        $writer = new FailedJobWriter($failer, new NullLogger);

        $this->assertFalse($writer->write(self::CONNECTION, 'default-quorum', '{}', new RuntimeException('boom')));
    }

    /**
     * @return array<string, array{string, string}>
     */
    public static function payloadsWithoutUuid(): array
    {
        return [
            'json without uuid' => ['{"data":{}}', '{"data":{}}'],
            'not json' => ['not json', 'not json'],
            'binary' => ["a\0b\xFFc", "a\0b\u{FFFD}c"],
        ];
    }

    #[DataProvider('payloadsWithoutUuid')]
    public function test_write_wraps_a_payload_without_uuid(string $payload, string $storedBody): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->with(self::CONNECTION, 'q', Mockery::on(function (string $stored) use ($storedBody) {
            $decoded = json_decode($stored, true);

            return is_string($decoded['uuid'] ?? null) && $decoded['body'] === $storedBody;
        }), Mockery::any())->andReturn(1);

        $writer = new FailedJobWriter($failer, new NullLogger);

        $this->assertTrue($writer->write(self::CONNECTION, 'q', $payload, new RuntimeException('boom')));
    }

    public function test_reject_message_writes_the_row_before_rejecting(): void
    {
        $exception = new RuntimeException('boom');
        $calls = [];
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->with(self::CONNECTION, 'q', self::PAYLOAD, $exception)
            ->andReturnUsing(function () use (&$calls) {
                $calls[] = 'log';

                return 1;
            });
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_reject')->once()->with(5, false)
            ->andReturnUsing(function () use (&$calls) {
                $calls[] = 'reject';
            });

        $writer = new FailedJobWriter($failer, new NullLogger);

        $this->assertTrue($writer->rejectMessage($this->makeMessage($channel), self::CONNECTION, 'q', $exception));
        $this->assertSame(['log', 'reject'], $calls);
    }

    public function test_reject_message_acks_when_asked(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->andReturn(1);
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_ack')->once()->with(5, false);
        $channel->shouldNotReceive('basic_reject');

        $writer = new FailedJobWriter($failer, new NullLogger);

        $this->assertTrue($writer->rejectMessage(
            $this->makeMessage($channel),
            self::CONNECTION,
            'q',
            new RuntimeException('boom'),
            true
        ));
    }

    public function test_reject_message_requeues_when_the_row_is_not_written(): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->andThrow(new RuntimeException('database is down'));
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_nack')->once()->with(5, false, true);
        $channel->shouldNotReceive('basic_reject');
        $channel->shouldNotReceive('basic_ack');

        $writer = new FailedJobWriter($failer, new NullLogger);

        $this->assertFalse($writer->rejectMessage(
            $this->makeMessage($channel),
            self::CONNECTION,
            'q',
            new RuntimeException('boom')
        ));
    }

    private function makeMessage(AMQPChannel $channel): AMQPMessage
    {
        $message = new AMQPMessage(self::PAYLOAD);
        $message->setChannel($channel);
        $message->setDeliveryInfo(5, false, '', 'q');

        return $message;
    }
}
