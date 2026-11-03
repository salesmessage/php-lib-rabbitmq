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
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;
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

    private ?\Throwable $confirmSelectFailure = null;

    /** @var list<int> numbers of the channel opens (counted from the test's start) that fail as with the broker down */
    private array $failingOpens = [];

    private bool $queueMissingOnGet = false;

    private bool $vhostMissing = false;

    /** @var array<int, true> channels the broker closed (a channel-level error): detached, not open */
    private array $closedByBroker = [];

    private AbstractConnection&Mockery\MockInterface $connection;

    private int $nextChannelId = 0;

    /** @var array<int, true> channels of a connection that reconnected on its own: still writable, frames lost */
    private array $staleWritable = [];

    /** @var array<int, true> channels php-amqplib closed on its side (do_close): not open, detached */
    private array $closedLocally = [];

    /** @var array<int, true> channels a reconnect of a connection only marked closed left open and attached */
    private array $leftAttached = [];

    /** A direct broker reports a missing vhost at connection.open, so in the reconnect itself. */
    private bool $vhostMissingOnConnect = false;

    /** Runs once, inside the next queue_declare: something that re-enters the queue meanwhile (a signal handler). */
    private ?\Closure $onQueueDeclare = null;

    /** False for a lazy connection that has never connected (php-amqplib: no server properties yet). */
    private bool $everConnected = true;

    private bool $basicPublishTimesOut = false;

    /** The broker takes every message, but the connection closes before its confirm arrives. */
    private bool $confirmWaitFails = false;

    /** @var list<float> pauses before a reconnect, in seconds (not slept in the tests) */
    private array $pauses = [];

    /** @var list<int> reconnects made before each pause */
    private array $reconnectsAtPause = [];

    /** Runs after each pause with the seconds paused so far: e.g. the broker comes back meanwhile. */
    private ?\Closure $whilePaused = null;

    /** @var list<array{0: int, 1: int}> basic_ack calls: channel id, delivery tag */
    private array $acked = [];

    private LoggerInterface&Mockery\MockInterface $logger;

    /** @var list<string> */
    private array $brokerExchanges = [];

    private VhostsService&Mockery\MockInterface $vhostsService;

    private LockService&Mockery\MockInterface $lockService;

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
        $this->vhostsService = Mockery::mock(VhostsService::class);
        $this->lockService = Mockery::mock(LockService::class);
        $container->instance(VhostsService::class, $this->vhostsService);
        $container->instance(LockService::class, $this->lockService);
        $this->logger = Mockery::spy(LoggerInterface::class);
        $container->instance(LoggerInterface::class, $this->logger);
        Container::setInstance($container);

        $queue = new class(QueueConfigFactory::make([])) extends RabbitMQQueueBatchable
        {
            public ?\Closure $onPause = null;

            protected function pauseBeforeReconnect(float $seconds): void
            {
                ($this->onPause)($seconds);
            }
        };
        $queue->onPause = $this->paused(...);
        $this->queue = $queue;
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
        // One reconnect by the idle guard, then the publish unit's own recoveries.
        $this->assertLessThanOrEqual(5, $this->reconnects);
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

    public function test_reconnect_opens_no_channel_and_drops_the_old_ones(): void
    {
        // The next use opens the channels inside its own retry; a channel opened here would leak when it does.
        $this->queue->getChannel();
        $this->queue->getChannel(false, true);
        $this->connected = false;
        $this->channelOpens = 0;

        $this->reconnect();

        $this->assertSame(0, $this->channelOpens);
        $this->assertNull((new ReflectionProperty($this->queue, 'channel'))->getValue($this->queue));
        $this->assertNull($this->confirmSelectedChannel());
    }

    public function test_after_a_reconnect_the_next_confirmed_publish_gets_a_new_channel_in_confirm_mode(): void
    {
        $oldConfirmChannel = $this->queue->getChannel(false, true);
        $this->connected = false;

        $this->reconnect();

        $this->assertNotSame($oldConfirmChannel, $this->queue->getChannel(false, true));
        $this->assertSame([1, 2], $this->confirmSelected);
    }

    public function test_recovery_of_a_new_channel_opens_only_the_channel_it_returns(): void
    {
        // What a vhost consumer does on start (initConnection) and pop() after a 404.
        $this->failingOpens = [1];
        $this->channelOpens = 0;

        $channel = $this->queue->getChannel(true);

        $this->assertSame(2, $this->channelOpens);
        $this->assertSame($channel, (new ReflectionProperty($this->queue, 'channel'))->getValue($this->queue));
    }

    public function test_publish_that_starts_while_another_publish_runs_retries_on_its_own(): void
    {
        // E.g. Laravel's timeout handler fails the job while its publish runs, and the job's failed() dispatches
        // the next job of its chain through the same queue.
        $this->publishesOnce(self::publishPaths()['push'][0]);
        $this->onQueueDeclare = function (): void {
            $this->connected = false;
            $this->queue->push(self::listenerJob(), '', self::QUEUE);
        };

        $this->queue->pushRaw(json_encode(['displayName' => 'OuterJob', 'id' => 'outer']), 'new-queue');

        $this->assertEqualsCanonicalizing(
            [['delivered', self::LISTENER], ['delivered', 'OuterJob']],
            $this->published
        );
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

        $deliveryChannel = $this->deliveryChannel(registered: true);
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

    public static function goneDeliveryChannels(): array
    {
        return [
            // Closed by php-amqplib when the connection was closed or replaced.
            'closed' => [false, null, [], 'delivery_channel_closed'],
            // Left attached by a reconnect after a missed heartbeat, but no longer one of the connection's channels;
            // the live channel that now has its id must not get the old delivery tag.
            'left behind by a reconnect' => [true, 'connection', ['live'], 'delivery_channel_replaced'],
        ];
    }

    #[DataProvider('goneDeliveryChannels')]
    public function test_delivery_from_before_a_reconnect_is_not_settled_and_does_not_fail(bool $open, ?string $connection, array $registered, string $reason): void
    {
        // The broker requeued the message when its channel went: nothing to settle, and no error for a job that
        // may well have succeeded.
        $live = Mockery::mock(AMQPChannel::class);
        $live->shouldNotReceive('basic_ack', 'basic_reject');
        $gone = Mockery::mock(AMQPChannel::class);
        $gone->shouldNotReceive('basic_ack', 'basic_reject');
        $gone->shouldReceive('is_open')->andReturn($open);
        $gone->shouldReceive('getChannelId')->andReturn(3);
        if ($connection === null) {
            $gone->shouldReceive('getConnection')->andReturnNull();
        } else {
            $amqpConnection = Mockery::mock(AbstractConnection::class);
            $amqpConnection->channels = in_array('live', $registered, true) ? [3 => $live] : [];
            $gone->shouldReceive('getConnection')->andReturn($amqpConnection);
        }
        $this->channelOpens = 0;

        $this->queue->ack($this->jobDeliveredOn($gone));
        $this->queue->reject($this->jobDeliveredOn($gone), true);

        $this->assertSame(0, $this->channelOpens);
        $this->logger->shouldHaveReceived('warning')->with('RabbitMQQueue.settle.skipped', Mockery::on(
            fn (array $context): bool => $context === ['reason' => $reason, 'delivery_tag' => 7, 'routing_key' => self::QUEUE]
        ))->twice();
    }

    public function test_job_finishes_without_error_when_its_own_publish_reconnected_the_connection(): void
    {
        // A vhost consumer delivered the job on the queue's channel; the job's publish found the connection closed
        // and its recovery reconnected, which closed that channel and requeued the delivery.
        $consumerChannel = $this->queue->getChannel(true);
        $message = new AMQPMessage(json_encode(['displayName' => self::LISTENER, 'job' => 'stdClass@call', 'data' => []]));
        $message->setChannel($consumerChannel);
        $message->setDeliveryInfo(7, false, '', self::QUEUE);
        $job = Mockery::mock(RabbitMQJob::class);
        $job->shouldReceive('getRabbitMQMessage')->andReturn($message);
        $this->connected = false;
        self::publishPaths()['push'][0]($this->queue, self::QUEUE);
        $this->assertSame(1, $this->reconnects);
        $this->acked = [];
        $this->channelOpens = 0;

        $this->queue->ack($job);

        // Nothing acked on the new channel either (the old delivery tag would make the broker close it).
        $this->assertSame([], $this->acked);
        $this->assertSame(0, $this->channelOpens);
    }

    private function jobDeliveredOn(AMQPChannel $channel): RabbitMQJob
    {
        $message = new AMQPMessage('{}');
        $message->setChannel($channel);
        $message->setDeliveryInfo(7, false, '', self::QUEUE);
        $job = Mockery::mock(RabbitMQJob::class);
        $job->shouldReceive('getRabbitMQMessage')->andReturn($message);

        return $job;
    }

    public static function vhostScenarios(): array
    {
        $scenarios = [];
        foreach (self::publishPaths() as $path => [$publish]) {
            // A first failure of another kind (the closed connection, a channel open failing upstream); only
            // the next channel open reports the vhost.
            $scenarios["$path, vhost reported after another failure"] = [$publish, false];
            // A reconnect failed while the broker was down; it is back without the vhost, and the next
            // publish's channel open reports it.
            $scenarios["$path, vhost reported after a failed reconnect"] = [$publish, true];
        }

        return $scenarios;
    }

    #[DataProvider('vhostScenarios')]
    public function test_publish_after_the_vhost_is_recreated_declares_its_queue_and_is_delivered(callable $publish, bool $afterFailedReconnect): void
    {
        $this->publishesOnce($publish);
        if ($afterFailedReconnect) {
            $this->brokerGoesDown();
            $this->publishFailure($publish, self::QUEUE);
            $this->upstreamDown = false;
        } else {
            // The first failure is of another kind (the closed connection, or the first channel open failing
            // upstream); only a later channel open reports the missing vhost.
            $this->connected = false;
            $this->channelOpens = 0;
            $this->failingOpens = [1];
        }
        $this->vhostMissing = true;
        $this->published = [];
        $this->expectVhostRecreation();

        $publish($this->queue, self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
    }

    public function test_vhost_still_missing_after_its_recreation_fails_the_publish_without_looping(): void
    {
        $publish = self::publishPaths()['push'][0];
        $this->publishesOnce($publish);
        $this->connected = false;
        $this->vhostMissing = true;
        $this->lockService->shouldReceive('lock')->andReturnUsing(function (string $key, \Closure $handler): bool {
            $handler();

            return true;
        });
        $this->vhostsService->shouldReceive('setConnection')->andReturnSelf();
        // The management API reports the vhost created, but the broker still doesn't have it: recreated once per
        // recovery, as on 1.83, within the publish's 4 recoveries.
        $this->vhostsService->shouldReceive('createVhost')->between(1, 4)->andReturnTrue();

        $exception = $this->publishFailure($publish, self::QUEUE);

        $this->assertInstanceOf(AMQPConnectionClosedException::class, $exception);
        $this->assertStringContainsString('vhost '.self::VHOST.' not found', $exception->getMessage());
    }

    #[DataProvider('publishPaths')]
    public function test_publish_after_another_process_recreated_the_vhost_declares_its_queue_and_is_delivered(callable $publish): void
    {
        // Another process saw the vhost missing and recreated it empty; this one sees only a closed connection.
        $this->publishesOnce($publish);
        $this->connected = false;
        $this->brokerQueues = [];
        $this->brokerExchanges = [];

        $publish($this->queue, self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
        // Only a connection found closed: recovered at once.
        $this->assertSame([], $this->pauses);
    }

    public function test_batch_is_published_whole_when_a_channel_open_inside_it_fails(): void
    {
        // The confirmed job needs the confirm channel, opened while the plain message is already buffered on the
        // main channel: the whole batch is built again on the new channels, not only the rest of it.
        $bulk = fn (array $jobs) => $this->queue->bulk($jobs, '', self::QUEUE);
        $bulk([self::listenerJob()]);
        $this->published = [];
        $this->channelOpens = 0;
        $this->failingOpens = [1];

        $bulk([self::listenerJob(), new ReconnectTestConfirmedJob]);

        $this->assertEqualsCanonicalizing(
            [['delivered', self::LISTENER], ['delivered', ReconnectTestConfirmedJob::class]],
            $this->published
        );
    }

    public function test_mixed_batch_whose_confirm_channel_open_reconnects_runs_again_on_new_channels(): void
    {
        // A failed write left the connection marked closed without detaching its channels; opening the confirm
        // channel makes php-amqplib connect again, which leaves the plain channel stale.
        $this->queue->bulk([self::listenerJob()], '', self::QUEUE);
        $this->published = [];
        foreach (array_keys($this->connection->channels) as $id) {
            $this->staleWritable[$id] = true;
        }
        $this->connected = false;

        $this->queue->bulk([self::listenerJob(), new ReconnectTestConfirmedJob], '', self::QUEUE);

        $this->assertEqualsCanonicalizing(
            [['delivered', self::LISTENER], ['delivered', ReconnectTestConfirmedJob::class]],
            $this->published
        );
    }

    public function test_channel_the_broker_closed_is_replaced_without_a_reconnect(): void
    {
        // A channel-level error closed the cached channel; reconnecting would also close a vhost consumer's
        // channel on the same connection.
        $publish = self::publishPaths()['pushRaw'][0];
        $this->publishesOnce($publish);
        foreach (array_keys($this->connection->channels) as $id) {
            $this->closedByBroker[$id] = true;
        }

        $publish($this->queue, self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
        $this->assertSame(0, $this->reconnects);
    }

    #[DataProvider('publishPaths')]
    public function test_publish_after_a_connection_level_close_into_a_recreated_vhost_declares_its_queue(callable $publish): void
    {
        // The broker closed the connection (e.g. read by a vhost consumer): php-amqplib closed every channel, and
        // the vhost was recreated empty before the next publish.
        $this->publishesOnce($publish);
        foreach (array_keys($this->connection->channels) as $id) {
            $this->closedByBroker[$id] = true;
        }
        $this->connected = false;
        $this->brokerQueues = [];
        $this->brokerExchanges = [];

        $publish($this->queue, self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
    }

    public function test_mixed_batch_whose_confirm_channel_open_reconnects_into_a_recreated_vhost_declares_again(): void
    {
        $this->queue->bulk([self::listenerJob()], '', self::QUEUE);
        $this->published = [];
        foreach (array_keys($this->connection->channels) as $id) {
            $this->staleWritable[$id] = true;
        }
        $this->connected = false;
        $this->brokerQueues = [];
        $this->brokerExchanges = [];

        $this->queue->bulk([self::listenerJob(), new ReconnectTestConfirmedJob], '', self::QUEUE);

        $this->assertEqualsCanonicalizing(
            [['delivered', self::LISTENER], ['delivered', ReconnectTestConfirmedJob::class]],
            $this->published
        );
    }

    public static function firstUses(): array
    {
        return [
            'push' => [fn (RabbitMQQueueBatchable $queue) => $queue->push(self::listenerJob(), '', self::QUEUE)],
            'pushRaw' => [fn (RabbitMQQueueBatchable $queue) => $queue->pushRaw(json_encode(['displayName' => self::LISTENER, 'id' => 'job-id']), self::QUEUE)],
            'a consumer opening its channel' => [fn (RabbitMQQueueBatchable $queue) => $queue->getChannel(true)],
            'size' => [fn (RabbitMQQueueBatchable $queue) => $queue->size(self::QUEUE)],
        ];
    }

    #[DataProvider('firstUses')]
    public function test_first_use_of_a_lazy_connection_is_not_taken_for_a_lost_connection(callable $use): void
    {
        // A lazy connection (the default) is not connected until its first channel.
        $this->connected = false;
        $this->everConnected = false;

        $use($this->queue);

        $this->assertSame(0, $this->reconnects);
        $this->logger->shouldNotHaveReceived('warning');
    }

    public function test_publish_whose_write_timed_out_surfaces_the_timeout_without_a_reconnect(): void
    {
        // As on 1.83: a write timeout is not a closed connection, so it is neither retried nor recovered, and the
        // next publish goes out on the same channel without a reconnect.
        $publish = self::publishPaths()['pushRaw'][0];
        $this->publishesOnce($publish);
        $channel = $this->queue->getChannel();
        $this->basicPublishTimesOut = true;
        try {
            $publish($this->queue, self::QUEUE);
            $this->fail('A write timeout must surface.');
        } catch (AMQPTimeoutException) {
        }

        $publish($this->queue, self::QUEUE);

        $this->assertSame(0, $this->reconnects);
        $this->assertSame([['delivered', self::LISTENER]], $this->published);
        $this->assertSame($channel, $this->queue->getChannel());
        $this->logger->shouldNotHaveReceived('warning');
    }

    public static function confirmSelectFailures(): array
    {
        return [
            // The close would wait on the same stalled socket: the channel is dropped without it.
            'write timeout' => [AMQPTimeoutException::writeTimeout(3), 0],
            'refused by the broker' => [new AMQPProtocolChannelException(406, 'PRECONDITION_FAILED - cannot switch from tx to confirm mode', [85, 10]), 1],
        ];
    }

    #[DataProvider('confirmSelectFailures')]
    public function test_confirm_channel_is_kept_only_in_confirm_mode(\Throwable $failure, int $closes): void
    {
        $this->confirmSelectFailure = $failure;

        try {
            $this->queue->getChannel(false, true);
            $this->fail('A failed confirm_select must surface.');
        } catch (\Throwable $exception) {
            $this->assertSame($failure, $exception);
        }

        $this->assertNull($this->confirmSelectedChannel());
        $this->assertSame($closes, $this->channelsClosed);
    }

    public function test_confirm_channel_the_broker_closed_is_replaced_without_a_reconnect(): void
    {
        $old = $this->queue->getChannel(false, true);
        $this->closedByBroker[$old->getChannelId()] = true;

        $new = $this->queue->getChannel(false, true);

        $this->assertNotSame($old, $new);
        $this->assertSame(0, $this->reconnects);
        $this->assertSame([1, 1], $this->confirmSelected);
    }

    public function test_vhost_the_reconnect_reports_missing_is_recreated_and_the_publish_delivered(): void
    {
        // On a direct broker the missing vhost shows up at connection.open, in the reconnect itself.
        $publish = self::publishPaths()['push'][0];
        $this->publishesOnce($publish);
        $this->connected = false;
        $this->vhostMissingOnConnect = true;
        $this->brokerQueues = [];
        $this->brokerExchanges = [];
        $this->expectVhostRecreation();

        $publish($this->queue, self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
    }

    #[DataProvider('publishPaths')]
    public function test_vhost_reported_missing_again_after_its_recreation_is_recreated_again(callable $publish): void
    {
        // E.g. deleted again right after the first recreation: as on 1.83, every recovery may recreate it.
        $this->publishesOnce($publish);
        $this->connected = false;
        $this->vhostMissing = true;
        $this->lockService->shouldReceive('lock')->andReturnUsing(function (string $key, \Closure $handler): bool {
            $handler();

            return true;
        });
        $this->vhostsService->shouldReceive('setConnection')->andReturnSelf();
        $recreations = 0;
        $this->vhostsService->shouldReceive('createVhost')->twice()->andReturnUsing(function () use (&$recreations): bool {
            if (++$recreations === 2) {
                $this->vhostMissing = false;
                $this->brokerQueues = [];
                $this->brokerExchanges = [];
            }

            return true;
        });

        $publish($this->queue, self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
    }

    public function test_channel_open_after_a_publish_still_recovers_on_its_own(): void
    {
        // The publish unit is over: a standalone channel open (a consumer's initConnection) retries again.
        $this->publishesOnce(self::publishPaths()['push'][0]);
        $this->failingOpens = [1];

        $this->queue->getChannel(true);

        $this->assertSame(1, $this->reconnects);
    }

    public function test_job_whose_message_has_no_channel_is_settled_on_the_queue_channel(): void
    {
        $channel = $this->queue->getChannel();
        $message = new AMQPMessage('{}');
        $message->setDeliveryInfo(7, false, '', self::QUEUE);
        $job = Mockery::mock(RabbitMQJob::class);
        $job->shouldReceive('getRabbitMQMessage')->andReturn($message);

        $this->queue->ack($job);

        $this->assertSame([[$channel->getChannelId(), 7]], $this->acked);
    }

    public function test_reconnect_of_a_connection_only_marked_closed_closes_the_channels_left_on_it(): void
    {
        // A vhost consumer reads on its own reference to a channel. A missed heartbeat marked the connection
        // closed without closing its channels, and the job's publish then reconnects it. Left open, the
        // consumer's channel would go on reading and settling on the new connection under an id that now belongs
        // to another channel; closed, the consumer sees its channel gone and opens a new one.
        $queueChannel = $this->queue->getChannel(true);
        $ownChannel = $this->connection->channel();
        $this->connected = false;

        self::publishPaths()['push'][0]($this->queue, self::QUEUE);

        $this->assertSame(1, $this->reconnects);
        $this->assertSame([['delivered', self::LISTENER]], $this->published);
        foreach ([$queueChannel, $ownChannel] as $channel) {
            $this->assertFalse($channel->is_open());
            $this->assertNull($channel->getConnection());
            $this->assertNotContains($channel, $this->connection->channels);
        }
    }

    public static function confirmedPublishes(): array
    {
        return [
            'push' => [fn (RabbitMQQueueBatchable $queue) => $queue->push(new ReconnectTestConfirmedJob, '', self::QUEUE)],
            'bulk' => [fn (RabbitMQQueueBatchable $queue) => $queue->bulk([new ReconnectTestConfirmedJob], '', self::QUEUE)],
        ];
    }

    #[DataProvider('confirmedPublishes')]
    public function test_publish_whose_confirm_keeps_failing_is_sent_at_most_as_often_as_on_1_83(callable $publish): void
    {
        // Every run wrote the message before the connection closed: each run after it sends it again (same
        // message_id). As on 1.83, at most two runs follow one that wrote, although failures before the write get
        // more attempts.
        $this->confirmWaitFails = true;

        try {
            $publish($this->queue);
            $this->fail('A publish whose confirm never arrives must throw.');
        } catch (AMQPConnectionClosedException $exception) {
            $this->assertStringContainsString('CONNECTION_FORCED', $exception->getMessage());
        }

        $this->assertCount(3, $this->published);
        // A connection the broker closed (not the proxy's UPSTREAM_ERROR) is recovered at once.
        $this->assertSame([], $this->pauses);
    }

    public function test_confirmed_push_whose_channel_opens_keep_failing_before_the_write_gets_every_recovery(): void
    {
        // Only opening the confirm channel fails (the broker behind the proxy restarting), three times in a row:
        // nothing was written, so these failures don't count against the re-sends after a write.
        $publish = fn () => $this->queue->push(new ReconnectTestConfirmedJob, '', self::QUEUE);
        $publish();
        $this->published = [];
        $this->connected = false;
        $this->channelOpens = 0;
        // Each run after a reconnect opens the plain channel (declare) and then the confirm channel (publish).
        $this->failingOpens = [2, 4, 6];

        $publish();

        $this->assertSame([['delivered', ReconnectTestConfirmedJob::class]], $this->published);
        $this->assertSame(4, $this->reconnects);
    }

    #[DataProvider('publishPaths')]
    public function test_publish_with_the_broker_down_waits_before_the_recoveries_after_the_first(callable $publish): void
    {
        // The first recovery is for the connection found closed (at once); the proxy then reports the broker
        // unreachable on every channel open, and the next recoveries wait before they reconnect: about 1.4 s in
        // all, then it throws.
        $this->publishesOnce($publish);
        $this->brokerGoesDown();

        $this->publishFailure($publish, self::QUEUE);

        $this->assertSame([0.2, 0.4, 0.8], $this->pauses);
        $this->assertSame([1, 2, 3], $this->reconnectsAtPause);
    }

    public function test_channel_open_with_the_broker_down_waits_only_before_its_second_recovery(): void
    {
        // A vhost consumer's initConnection: two recoveries, then it throws and the consumer waits on its own. A
        // single UPSTREAM_ERROR (the proxy lost its upstream connection, the broker is up) reconnects at once.
        $this->upstreamDown = true;

        try {
            $this->queue->getChannel(true);
            $this->fail('Opening a channel with the broker down must throw.');
        } catch (AMQPConnectionClosedException) {
        }

        $this->assertSame([0.2], $this->pauses);
    }

    public function test_single_upstream_error_while_the_broker_is_up_reconnects_at_once(): void
    {
        $this->failingOpens = [1];

        $this->queue->getChannel(true);

        $this->assertSame(1, $this->reconnects);
        $this->assertSame([], $this->pauses);
    }

    #[DataProvider('publishPaths')]
    public function test_publish_while_the_broker_restarts_behind_the_proxy_waits_for_it_and_is_delivered(callable $publish): void
    {
        // The broker is back about a second after the connection dropped: the pauses ride it through.
        $this->publishesOnce($publish);
        $this->brokerGoesDown();
        $this->whilePaused = function (float $paused): void {
            if ($paused >= 1.0) {
                $this->upstreamDown = false;
            }
        };

        $publish($this->queue, self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
    }

    private function paused(float $seconds): void
    {
        $this->pauses[] = $seconds;
        $this->reconnectsAtPause[] = $this->reconnects;
        if ($this->whilePaused !== null) {
            ($this->whilePaused)(array_sum($this->pauses));
        }
    }

    private function expectVhostRecreation(): void
    {
        $this->lockService->shouldReceive('lock')->andReturnUsing(function (string $key, \Closure $handler): bool {
            $handler();

            return true;
        });
        $this->vhostsService->shouldReceive('setConnection')->andReturnSelf();
        $this->vhostsService->shouldReceive('createVhost')->once()->with(self::VHOST, Mockery::any())
            ->andReturnUsing(function (): bool {
                $this->vhostMissing = false;
                $this->vhostMissingOnConnect = false;
                // The recreated vhost is empty.
                $this->brokerQueues = [];
                $this->brokerExchanges = [];

                return true;
            });
    }

    private function deliveryChannel(bool $registered): AMQPChannel&Mockery\MockInterface
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->channels = $registered ? [3 => $channel] : [3 => Mockery::mock(AMQPChannel::class)];
        $channel->shouldReceive('getConnection')->andReturn($connection);
        $channel->shouldReceive('getChannelId')->andReturn(3);
        $channel->shouldReceive('is_open')->andReturnTrue();

        return $channel;
    }

    private function publishesOnce(callable $publish): void
    {
        $publish($this->queue, self::QUEUE);
        $this->assertSame([['delivered', self::LISTENER]], $this->published);
        $this->published = [];
        $this->reconnects = 0;
        $this->channelOpens = 0;
    }

    /**
     * The proxy closes the client connection when its upstream goes away, and every new channel then fails.
     * This is the state once php-amqplib has read the proxy's Connection.Close (a confirm, a declare, a channel
     * open, the idle probe). A plain publish on an established connection reads nothing: it is written into
     * the closed socket and lost without an error, as on 1.83; that case is not covered here.
     */
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
        $connection->channels = [];
        $this->connection = $connection;
        $connection->shouldReceive('isConnected')->andReturnUsing(fn () => $this->connected);
        $connection->shouldReceive('getServerProperties')->andReturnUsing(fn () => $this->everConnected ? ['product' => 'RabbitMQ'] : null);
        $connection->shouldReceive('getHeartbeat')->andReturn(30);
        $connection->shouldReceive('getLastActivity')->andReturnUsing(fn () => $this->lastActivity);
        // The proxy answers the handshake itself, whether or not the broker behind it is up.
        $connection->shouldReceive('reconnect')->andReturnUsing(function (): void {
            $this->leaveOrCloseChannelsForANewConnection();
            if ($this->vhostMissingOnConnect) {
                $this->connected = false;

                throw new AMQPConnectionClosedException('NOT_ALLOWED - vhost '.self::VHOST.' not found(10, 40)', 530);
            }
            $this->connected = true;
            $this->everConnected = true;
            $this->generation++;
            $this->reconnects++;
            $this->lastActivity = $this->secondsAgo(0);
        });
        $connection->shouldReceive('channel')->andReturnUsing(fn () => $this->openChannel());

        return $connection;
    }

    /**
     * php-amqplib's reconnect: on a live connection it closes every channel first; on one only marked closed
     * (a missed heartbeat, a failed write) close() returns early and the channels stay open and attached,
     * while the connection starts a new channel list.
     */
    private function leaveOrCloseChannelsForANewConnection(): void
    {
        foreach (array_keys($this->connection->channels) as $id) {
            if ($this->connected) {
                $this->closeLocally($id);
            } elseif (! isset($this->closedByBroker[$id])) {
                $this->leftAttached[$id] = true;
            }
        }
        $this->connection->channels = [];
    }

    /** The peer's Connection.Close, read by php-amqplib: the connection and all its channels are closed. */
    private function connectionClosedByPeer(): void
    {
        foreach (array_keys($this->connection->channels) as $id) {
            $this->closeLocally($id);
        }
        $this->connected = false;
    }

    /** php-amqplib's do_close(): not open, detached, off its connection's channel list. */
    private function closeLocally(int $id): void
    {
        $this->closedLocally[$id] = true;
        unset($this->connection->channels[$id]);
    }

    private function openChannel(): AMQPChannel
    {
        if (++$this->channelOpens > self::MAX_CHANNEL_OPENS) {
            throw new LogicException('Unbounded reconnect recursion: '.self::MAX_CHANNEL_OPENS.' channel opens.');
        }

        if (! $this->connected) {
            // php-amqplib connects again before opening a channel on a closed connection; the proxy accepts it.
            $this->leaveOrCloseChannelsForANewConnection();
            $this->connected = true;
            $this->everConnected = true;
            $this->generation++;
        }

        if ($this->upstreamDown || in_array($this->channelOpens, $this->failingOpens, true)) {
            // AMQProxy closes the whole client connection when it cannot open the channel upstream; php-amqplib
            // then closes every channel of it (connection_close → do_close).
            $this->connectionClosedByPeer();

            throw new AMQPConnectionClosedException("UPSTREAM_ERROR - Error connecting to 'rabbitmq:5672': Connection refused(0, 0)", 503);
        }

        if ($this->vhostMissing) {
            // Behind AMQProxy a missing vhost shows up only when the proxy opens the channel upstream.
            $this->connectionClosedByPeer();

            throw new AMQPConnectionClosedException('NOT_ALLOWED - vhost '.self::VHOST.' not found(0, 0)', 403);
        }

        $generation = $this->generation;
        $id = ++$this->nextChannelId;
        $alive = fn (): bool => $this->connected && ! isset($this->closedByBroker[$id]) && ! isset($this->closedLocally[$id])
            && ($generation === $this->generation || isset($this->staleWritable[$id]));
        $deliver = function (AMQPMessage $message, string $exchange = '', string $routingKey = '', bool $checked = false) use ($alive, $id): void {
            if (! $checked && ! $alive()) {
                // php-amqplib checks the connection before every publish (AMQPChannel::checkConnection).
                throw new AMQPChannelClosedException('Channel connection is closed.');
            }
            if (isset($this->staleWritable[$id])) {
                // The frame goes out with a channel id the new connection doesn't have: the broker drops it and
                // closes the connection.
                $this->published[] = ['lost', json_decode($message->getBody(), true)['displayName'] ?? null];
                $this->connected = false;

                return;
            }
            // A message reaches only a queue the broker has: an unroutable one on a channel without a return
            // listener is dropped. A delayed message goes through the delay exchange and its queue.
            $routed = in_array($routingKey, $this->brokerQueues, true)
                && ($exchange === '' || (in_array($exchange, $this->brokerExchanges, true) && in_array($exchange, $this->brokerQueues, true)));
            $this->published[] = [$routed ? 'delivered' : 'lost', json_decode($message->getBody(), true)['displayName'] ?? null];
        };

        $channel = Mockery::mock(AMQPChannel::class)->shouldIgnoreMissing();
        $channel->shouldReceive('queue_declare')->andReturnUsing(function (string $name, bool $passive) use ($alive): array {
            if ($hook = $this->onQueueDeclare) {
                $this->onQueueDeclare = null;
                $hook();
            }
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
            if ($this->confirmSelectFailure !== null) {
                throw $this->confirmSelectFailure;
            }
            $this->confirmSelected[] = $generation;
        });
        $channel->shouldReceive('close')->andReturnUsing(function () use ($id): void {
            $this->channelsClosed++;
            $this->closeLocally($id);
        });
        // php-amqplib's own: closes the channel without I/O when its connection is marked closed.
        $channel->shouldReceive('closeIfDisconnected')->andReturnUsing(function () use ($id): bool {
            if ($this->connected || isset($this->closedLocally[$id])) {
                return false;
            }
            $this->closeLocally($id);

            return true;
        });
        // Detached when closed; still attached after its connection reconnected without closing it.
        $channel->shouldReceive('getConnection')->andReturnUsing(
            fn () => ! isset($this->closedByBroker[$id]) && ! isset($this->closedLocally[$id])
                && ($generation === $this->generation || isset($this->staleWritable[$id]) || isset($this->leftAttached[$id]))
                ? $this->connection
                : null
        );
        $channel->shouldReceive('is_open')->andReturnUsing(fn (): bool => ! isset($this->closedByBroker[$id]) && ! isset($this->closedLocally[$id]));
        $channel->shouldReceive('getChannelId')->andReturn($id);
        $channel->shouldReceive('basic_get')->andReturnUsing(function (string $queue) {
            if ($this->queueMissingOnGet) {
                $this->queueMissingOnGet = false;

                throw new AMQPProtocolChannelException(404, "NOT_FOUND - no queue '$queue'", [60, 70]);
            }

            return null;
        });
        $channel->shouldReceive('wait_for_pending_acks_returns')->andReturnUsing(function (): void {
            if ($this->confirmWaitFails) {
                $this->connectionClosedByPeer();

                throw new AMQPConnectionClosedException("CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'(0, 0)", 320);
            }
        });
        $channel->shouldReceive('basic_ack')->andReturnUsing(function (int $deliveryTag) use ($id): void {
            $this->acked[] = [$id, $deliveryTag];
        });
        $channel->shouldReceive('basic_publish')->andReturnUsing(function (AMQPMessage $message, string $exchange = '', string $routingKey = '') use ($deliver): void {
            if ($this->basicPublishTimesOut) {
                $this->basicPublishTimesOut = false;

                throw AMQPTimeoutException::writeTimeout(3);
            }
            $deliver($message, $exchange, $routingKey);
        });
        // A batch is only buffered on the channel; publish_batch writes it.
        // The batch lives in php-amqplib's own buffer, which publish_batch empties only after a successful write.
        $buffer = fn () => (fn () => $this->batch_messages)->call($channel);
        $setBuffer = function (array $messages) use ($channel): void {
            (function () use ($messages): void {
                $this->batch_messages = $messages;
            })->call($channel);
        };
        $channel->shouldReceive('batch_basic_publish')->andReturnUsing(function (AMQPMessage $message, string $exchange = '', string $routingKey = '') use ($buffer, $setBuffer): void {
            $setBuffer([...$buffer(), [$message, $exchange, $routingKey]]);
        });
        // php-amqplib checks the connection once, then writes the whole batch in one go.
        $channel->shouldReceive('publish_batch')->andReturnUsing(function () use ($buffer, $setBuffer, $deliver, $alive): void {
            if (! $alive()) {
                throw new AMQPChannelClosedException('Channel connection is closed.');
            }
            foreach ($buffer() as [$message, $exchange, $routingKey]) {
                $deliver($message, $exchange, $routingKey, checked: true);
            }
            $setBuffer([]);
        });
        $channel->shouldReceive('exchange_declare')->andReturnUsing(function (string $name, string $type, bool $passive = false) use ($alive) {
            if (! $alive()) {
                throw new AMQPChannelClosedException('Channel connection is closed.');
            }
            if (! in_array($name, $this->brokerExchanges, true)) {
                if ($passive) {
                    throw new AMQPProtocolChannelException(404, "NOT_FOUND - no exchange '$name'", [40, 10]);
                }
                $this->brokerExchanges[] = $name;
            }

            return null;
        });

        $this->connection->channels[$id] = $channel;

        return $channel;
    }
}

class ReconnectTestConfirmedJob implements RabbitMQConsumable
{
    public function isDuplicated(): bool
    {
        return false;
    }

    public function getQueueType(): string
    {
        return RabbitMQConsumable::MQ_TYPE_QUORUM;
    }

    public function shouldConfirmOnPublish(): bool
    {
        return true;
    }
}
