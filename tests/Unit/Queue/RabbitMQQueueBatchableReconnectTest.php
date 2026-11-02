<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Queue;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Events\CallQueuedListener;
use Illuminate\Support\Carbon;
use LogicException;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionMethod;
use ReflectionProperty;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob;
use Salesmessage\LibRabbitMQ\Queue\QueueConfigFactory;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueueBatchable;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Lock\LockService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

/**
 * A proxy such as AMQProxy completes the AMQP handshake itself, so with the broker behind it down a reconnect
 * succeeds and only opening a channel fails (Connection.Close UPSTREAM_ERROR). The reconnect must then give up
 * with that exception after its own attempts instead of recovering again from inside the recovery.
 */
class RabbitMQQueueBatchableReconnectTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const LISTENER = 'App\\Listeners\\SendHubSpotTimelineEvent';

    private const QUEUE = 'hubspot-listener';

    private const VHOST = 'organization_1';

    /** Far above any bounded retry; stops an unbounded recursion before it exhausts the memory. */
    private const MAX_CHANNEL_OPENS = 50;

    private RabbitMQQueueBatchable $queue;

    private bool $connected = true;

    private bool $upstreamDown = false;

    private bool $confirmSelectTimesOut = false;

    /** @var list<int> numbers of the channel opens (counted from the test's start) that fail as with the broker down */
    private array $failingOpens = [];

    private bool $queueMissingOnGet = false;

    private int $channelsClosed = 0;

    private int $generation = 1;

    private int $reconnects = 0;

    private int $channelOpens = 0;

    private float $lastActivity;

    /** @var list<string> */
    private array $brokerQueues = [self::QUEUE];

    /** @var list<array{0: string, 1: string|null}> */
    private array $published = [];

    /** @var list<int> generations of the channels put into confirm mode */
    private array $confirmSelected = [];

    protected function setUp(): void
    {
        parent::setUp();

        Carbon::setTestNow(Carbon::parse('2026-11-02 12:00:00'));
        $this->lastActivity = $this->secondsAgo(1);

        $container = new Container;
        $container->instance('config', new Repository([]));
        $container->instance(InternalStorageManager::class, Mockery::mock(InternalStorageManager::class)->shouldIgnoreMissing());
        $container->instance(GroupsService::class, Mockery::mock(GroupsService::class));
        $container->instance(VhostsService::class, Mockery::mock(VhostsService::class));
        $container->instance(LockService::class, Mockery::mock(LockService::class));
        $container->instance(LoggerInterface::class, Mockery::spy(LoggerInterface::class));
        Container::setInstance($container);

        $this->queue = new RabbitMQQueueBatchable(QueueConfigFactory::make([]));
        $this->queue->setContainer($container);
        $this->queue->setConnectionName('rabbitmq_cluster:vhost:'.self::VHOST);
        $this->queue->setConnection($this->fakeConnection());
    }

    protected function tearDown(): void
    {
        Carbon::setTestNow();
        Container::setInstance(null);

        parent::tearDown();
    }

    public static function publishPaths(): array
    {
        return [
            'push' => [fn (RabbitMQQueueBatchable $queue, string $queueName) => $queue->push(self::listenerJob(), '', $queueName)],
            'pushRaw' => [fn (RabbitMQQueueBatchable $queue, string $queueName) => $queue->pushRaw(
                json_encode(['displayName' => self::LISTENER, 'id' => 'job-id']),
                $queueName
            )],
            'bulk' => [fn (RabbitMQQueueBatchable $queue, string $queueName) => $queue->bulk([self::listenerJob()], '', $queueName)],
            'laterRaw with a delay' => [fn (RabbitMQQueueBatchable $queue, string $queueName) => $queue->laterRaw(
                30,
                json_encode(['displayName' => self::LISTENER, 'id' => 'job-id']),
                $queueName
            )],
        ];
    }

    #[DataProvider('publishPaths')]
    public function test_publish_with_the_broker_down_behind_the_proxy_fails_with_a_closed_connection(callable $publish): void
    {
        $this->publishesOnce($publish);
        $this->brokerGoesDown();

        $exception = $this->publishFailure($publish, self::QUEUE);

        $this->assertInstanceOf(AMQPConnectionClosedException::class, $exception);
        $this->assertStringContainsString('UPSTREAM_ERROR', $exception->getMessage());
        $this->assertLessThanOrEqual(4, $this->reconnects);
    }

    #[DataProvider('publishPaths')]
    public function test_publish_to_a_new_queue_with_the_broker_down_fails_with_a_closed_connection(callable $publish): void
    {
        // The queue is not declared yet, so the publish opens a channel for the declare first.
        $this->publishesOnce($publish);
        $this->brokerGoesDown();

        $exception = $this->publishFailure($publish, 'new-queue');

        $this->assertInstanceOf(AMQPConnectionClosedException::class, $exception);
        $this->assertLessThanOrEqual(4, $this->reconnects);
    }

    #[DataProvider('publishPaths')]
    public function test_publish_after_the_broker_is_back_reconnects_and_is_delivered(callable $publish): void
    {
        $this->publishesOnce($publish);
        $this->brokerGoesDown();
        $this->publishFailure($publish, self::QUEUE);

        $this->upstreamDown = false;
        $this->published = [];
        $publish($this->queue, self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
    }

    #[DataProvider('publishPaths')]
    public function test_publish_to_a_new_queue_after_the_broker_is_back_is_delivered(callable $publish): void
    {
        // The failed reconnect must not leave the dead channels behind for the declare of a new queue.
        $this->publishesOnce($publish);
        $this->brokerGoesDown();
        $this->publishFailure($publish, self::QUEUE);

        $this->upstreamDown = false;
        $this->published = [];
        $publish($this->queue, 'new-queue');

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
    }

    public function test_idle_guard_with_the_broker_down_behind_the_proxy_fails_with_a_closed_connection(): void
    {
        $publish = self::publishPaths()['push'][0];
        $this->publishesOnce($publish);
        // The connection still looks alive and idle for longer than the heartbeat: the guard opens a channel.
        $this->upstreamDown = true;
        $this->lastActivity = $this->secondsAgo(45);

        $exception = $this->publishFailure($publish, self::QUEUE);

        $this->assertInstanceOf(AMQPConnectionClosedException::class, $exception);
        $this->assertLessThanOrEqual(2, $this->reconnects);
    }

    public function test_new_channel_with_the_broker_down_behind_the_proxy_fails_with_a_closed_connection(): void
    {
        // What a vhost consumer does on start (initConnection) and pop() after a 404.
        $this->upstreamDown = true;

        try {
            $this->queue->getChannel(true);
            $this->fail('Opening a channel with the broker down must throw.');
        } catch (AMQPConnectionClosedException $exception) {
            $this->assertStringContainsString('UPSTREAM_ERROR', $exception->getMessage());
        }

        $this->assertLessThanOrEqual(2, $this->reconnects);
    }

    public function test_reconnect_puts_the_new_confirm_channel_into_confirm_mode(): void
    {
        $oldConfirmChannel = $this->queue->getChannel(false, true);
        $this->connected = false;

        $this->reconnect();

        $this->assertNotSame($oldConfirmChannel, $this->confirmSelectedChannel());
        $this->assertSame([1, 2], $this->confirmSelected);
    }

    public function test_reconnect_does_not_keep_a_confirm_channel_without_confirm_mode(): void
    {
        $this->queue->getChannel(false, true);
        $this->connected = false;
        $this->confirmSelectTimesOut = true;

        try {
            $this->reconnect();
            $this->fail('A confirm_select timeout must surface.');
        } catch (AMQPTimeoutException) {
        }

        // No channel without confirm mode is kept: the next confirmed publish opens a new one in confirm mode.
        $this->assertNull($this->confirmSelectedChannel());
        $this->confirmSelectTimesOut = false;
        $this->queue->getChannel(false, true);
        $this->assertSame([1, 2], $this->confirmSelected);
    }

    public function test_failed_reconnect_does_not_keep_a_main_channel_of_the_closed_connection(): void
    {
        // Each attempt opens the main channel and then fails on the confirm channel: the connection is closed,
        // so the main channel is dead as well and must not be kept for the next use.
        $this->queue->getChannel();
        $this->queue->getChannel(false, true);
        $this->connected = false;
        $this->channelOpens = 0;
        $this->failingOpens = [2, 4];

        try {
            $this->reconnect();
            $this->fail('The reconnect must fail when every attempt fails.');
        } catch (AMQPConnectionClosedException) {
        }

        $this->assertNull((new ReflectionProperty($this->queue, 'channel'))->getValue($this->queue));
        $this->assertNull($this->confirmSelectedChannel());
    }

    public static function settlements(): array
    {
        return [
            'ack' => ['basic_ack', [7], fn (RabbitMQQueueBatchable $queue, RabbitMQJob $job) => $queue->ack($job)],
            'reject' => ['basic_reject', [7, true], fn (RabbitMQQueueBatchable $queue, RabbitMQJob $job) => $queue->reject($job, true)],
        ];
    }

    #[DataProvider('settlements')]
    public function test_job_is_settled_on_the_channel_it_was_delivered_on(string $method, array $arguments, callable $settle): void
    {
        // A failed reconnect while the job ran leaves the queue without channels; the delivery tag belongs to
        // the channel that delivered the message, so it must not be sent on a new one.
        $publish = self::publishPaths()['pushRaw'][0];
        $this->publishesOnce($publish);
        $this->brokerGoesDown();
        $this->publishFailure($publish, self::QUEUE);
        $this->upstreamDown = false;
        $this->channelOpens = 0;

        $deliveryChannel = Mockery::mock(AMQPChannel::class);
        $deliveryChannel->shouldReceive($method)->once()->with(...$arguments);
        $message = new AMQPMessage('{}');
        $message->setChannel($deliveryChannel);
        $message->setDeliveryInfo(7, false, '', self::QUEUE);
        $job = Mockery::mock(RabbitMQJob::class);
        $job->shouldReceive('getRabbitMQMessage')->andReturn($message);

        $settle($this->queue, $job);

        $this->assertSame(0, $this->channelOpens);
    }

    public function test_pop_reports_a_lost_connection_when_the_channel_cannot_be_reopened_after_a_missing_queue(): void
    {
        // pop() reopens the channel the 404 closed; with the broker down that fails, and the worker must see a
        // lost connection so that it restarts.
        $this->queue->getChannel();
        $this->queueMissingOnGet = true;
        $this->upstreamDown = true;

        try {
            $this->queue->pop(self::QUEUE);
            $this->fail('pop() must throw when the channel cannot be reopened.');
        } catch (AMQPRuntimeException $exception) {
            $this->assertStringStartsWith('Lost connection: ', $exception->getMessage());
        }
    }

    public function test_failed_reconnect_attempt_closes_the_channels_it_opened(): void
    {
        $this->queue->getChannel(false, true);
        $this->connected = false;
        $this->confirmSelectTimesOut = true;
        $this->channelsClosed = 0;

        try {
            $this->reconnect();
            $this->fail('A confirm_select timeout must surface.');
        } catch (AMQPTimeoutException) {
        }

        // The main and the confirm channel of the failed attempt.
        $this->assertSame(2, $this->channelsClosed);
    }

    private function publishesOnce(callable $publish): void
    {
        $publish($this->queue, self::QUEUE);
        $this->assertSame([['delivered', self::LISTENER]], $this->published);
        $this->published = [];
        $this->reconnects = 0;
        $this->channelOpens = 0;
    }

    /** The proxy closes the client connection when its upstream goes away, and every new channel then fails. */
    private function brokerGoesDown(): void
    {
        $this->upstreamDown = true;
        $this->connected = false;
    }

    private function publishFailure(callable $publish, string $queueName): \Throwable
    {
        try {
            $publish($this->queue, $queueName);
        } catch (LogicException $exception) {
            throw $exception;
        } catch (\Throwable $exception) {
            $this->assertSame([], $this->published);

            return $exception;
        }

        $this->fail('Publishing with the broker down must throw.');
    }

    private function reconnect(): void
    {
        (new ReflectionMethod($this->queue, 'reconnect'))->invoke($this->queue, 2);
    }

    private function confirmSelectedChannel(): ?AMQPChannel
    {
        return (new ReflectionProperty($this->queue, 'confirmChannel'))->getValue($this->queue);
    }

    private static function listenerJob(): CallQueuedListener
    {
        return new CallQueuedListener(self::LISTENER, 'handle', []);
    }

    private function secondsAgo(float $seconds): float
    {
        return Carbon::now()->getPreciseTimestamp() / 1_000_000 - $seconds;
    }

    private function fakeConnection(): AbstractConnection
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('isConnected')->andReturnUsing(fn () => $this->connected);
        $connection->shouldReceive('getHeartbeat')->andReturn(30);
        $connection->shouldReceive('getLastActivity')->andReturnUsing(fn () => $this->lastActivity);
        // The proxy answers the handshake itself, whether or not the broker behind it is up.
        $connection->shouldReceive('reconnect')->andReturnUsing(function (): void {
            $this->connected = true;
            $this->generation++;
            $this->reconnects++;
            $this->lastActivity = $this->secondsAgo(0);
        });
        $connection->shouldReceive('channel')->andReturnUsing(fn () => $this->openChannel());

        return $connection;
    }

    private function openChannel(): AMQPChannel
    {
        if (++$this->channelOpens > self::MAX_CHANNEL_OPENS) {
            throw new LogicException('Unbounded reconnect recursion: '.self::MAX_CHANNEL_OPENS.' channel opens.');
        }

        if (! $this->connected) {
            // php-amqplib connects again before opening a channel on a closed connection; the proxy accepts it.
            $this->connected = true;
            $this->generation++;
        }

        if ($this->upstreamDown || in_array($this->channelOpens, $this->failingOpens, true)) {
            // AMQProxy closes the whole client connection when it cannot open the channel upstream.
            $this->connected = false;

            throw new AMQPConnectionClosedException('UPSTREAM_ERROR(0, 0)', 500);
        }

        $generation = $this->generation;
        $alive = fn (): bool => $this->connected && $generation === $this->generation;
        $deliver = function (AMQPMessage $message) use ($alive): void {
            if (! $alive()) {
                // php-amqplib checks the connection before every publish (AMQPChannel::checkConnection).
                throw new AMQPChannelClosedException('Channel connection is closed.');
            }
            $this->published[] = ['delivered', json_decode($message->getBody(), true)['displayName'] ?? null];
        };
        $batch = [];

        $channel = Mockery::mock(AMQPChannel::class)->shouldIgnoreMissing();
        $channel->shouldReceive('queue_declare')->andReturnUsing(function (string $name, bool $passive) use ($alive): array {
            if (! $alive()) {
                throw new AMQPChannelClosedException('Channel connection is closed.');
            }
            if (! in_array($name, $this->brokerQueues, true)) {
                if ($passive) {
                    throw new AMQPProtocolChannelException(404, "NOT_FOUND - no queue '$name'", [50, 10]);
                }
                $this->brokerQueues[] = $name;
            }

            return [$name, 0, 0];
        });
        $channel->shouldReceive('confirm_select')->andReturnUsing(function () use ($generation): void {
            if ($this->confirmSelectTimesOut) {
                throw new AMQPTimeoutException('The connection timed out after 3 sec while awaiting incoming data');
            }
            $this->confirmSelected[] = $generation;
        });
        $channel->shouldReceive('close')->andReturnUsing(function (): void {
            $this->channelsClosed++;
        });
        $channel->shouldReceive('basic_get')->andReturnUsing(function (string $queue) {
            if ($this->queueMissingOnGet) {
                $this->queueMissingOnGet = false;

                throw new AMQPProtocolChannelException(404, "NOT_FOUND - no queue '$queue'", [60, 70]);
            }

            return null;
        });
        $channel->shouldReceive('basic_publish')->andReturnUsing($deliver);
        // A batch is only buffered on the channel; publish_batch writes it.
        $channel->shouldReceive('batch_basic_publish')->andReturnUsing(function (AMQPMessage $message) use (&$batch): void {
            $batch[] = $message;
        });
        $channel->shouldReceive('publish_batch')->andReturnUsing(function () use (&$batch, $deliver): void {
            $messages = $batch;
            $batch = [];
            array_map($deliver, $messages);
        });

        return $channel;
    }
}
