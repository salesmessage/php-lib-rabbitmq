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

    private bool $confirmSelectTimesOut = false;

    /** @var list<int> numbers of the channel opens (counted from the test's start) that fail as with the broker down */
    private array $failingOpens = [];

    private bool $queueMissingOnGet = false;

    private bool $vhostMissing = false;

    private bool $publishBatchTimesOut = false;

    private AbstractConnection&Mockery\MockInterface $connection;

    private int $nextChannelId = 0;

    /** @var array<int, true> channels of a connection that reconnected on its own: still writable, frames lost */
    private array $staleWritable = [];

    /** Runs once, inside the next queue_declare: something that re-enters the queue meanwhile (a signal handler). */
    private ?\Closure $onQueueDeclare = null;

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

    public function test_publish_on_a_connection_that_reconnected_on_its_own_uses_a_new_channel(): void
    {
        $publish = self::publishPaths()['pushRaw'][0];
        $this->publishesOnce($publish);
        $this->reconnectOnItsOwnAfterAMissedHeartbeat();

        $publish($this->queue, self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
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

    public function test_batch_whose_confirm_channel_fails_leaves_nothing_buffered_for_the_next_batch(): void
    {
        $this->confirmSelectTimesOut = true;
        try {
            $this->queue->bulk([self::listenerJob(), new ReconnectTestConfirmedJob], '', self::QUEUE);
            $this->fail('A confirm_select timeout must surface.');
        } catch (AMQPTimeoutException) {
        }
        $this->confirmSelectTimesOut = false;
        $this->published = [];

        $this->queue->bulk([self::listenerJob()], '', self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
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

    public function test_stale_delivery_channel_left_by_a_reconnect_is_not_used_to_settle_the_job(): void
    {
        // After a missed heartbeat php-amqplib reconnects without closing the old channel objects: an ack on one
        // would go out on the new connection with a channel id it doesn't own.
        $deliveryChannel = $this->deliveryChannel(registered: false);
        $deliveryChannel->shouldNotReceive('basic_ack');
        $message = new AMQPMessage('{}');
        $message->setChannel($deliveryChannel);
        $message->setDeliveryInfo(7, false, '', self::QUEUE);
        $job = Mockery::mock(RabbitMQJob::class);
        $job->shouldReceive('getRabbitMQMessage')->andReturn($message);

        $this->expectException(AMQPChannelClosedException::class);
        $this->queue->ack($job);
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
        // The management API reports the vhost created, but the broker still doesn't have it.
        $this->vhostsService->shouldReceive('createVhost')->once()->andReturnTrue();

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

    public function test_batch_whose_write_times_out_leaves_nothing_for_the_next_batch(): void
    {
        $this->publishBatchTimesOut = true;
        try {
            $this->queue->bulk([self::listenerJob()], '', self::QUEUE);
            $this->fail('A write timeout must surface.');
        } catch (AMQPTimeoutException) {
        }
        $this->published = [];

        $this->queue->bulk([new ReconnectTestConfirmedJob], '', self::QUEUE);
        $this->queue->bulk([self::listenerJob()], '', self::QUEUE);

        $this->assertSame([['delivered', ReconnectTestConfirmedJob::class], ['delivered', self::LISTENER]], $this->published);
    }

    public function test_confirm_channel_is_kept_only_in_confirm_mode(): void
    {
        $this->confirmSelectTimesOut = true;

        try {
            $this->queue->getChannel(false, true);
            $this->fail('A confirm_select timeout must surface.');
        } catch (AMQPTimeoutException) {
        }

        $this->assertNull($this->confirmSelectedChannel());
        // Not left open on the connection either.
        $this->assertSame(1, $this->channelsClosed);
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
        $connection->channels = [];
        $this->connection = $connection;
        $connection->shouldReceive('isConnected')->andReturnUsing(fn () => $this->connected);
        $connection->shouldReceive('getHeartbeat')->andReturn(30);
        $connection->shouldReceive('getLastActivity')->andReturnUsing(fn () => $this->lastActivity);
        // The proxy answers the handshake itself, whether or not the broker behind it is up.
        $connection->shouldReceive('reconnect')->andReturnUsing(function (): void {
            $this->connected = true;
            $this->connection->channels = [];
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
            $this->connection->channels = [];
            $this->generation++;
        }

        if ($this->upstreamDown || in_array($this->channelOpens, $this->failingOpens, true)) {
            // AMQProxy closes the whole client connection when it cannot open the channel upstream.
            $this->connected = false;

            throw new AMQPConnectionClosedException('UPSTREAM_ERROR(0, 0)', 500);
        }

        if ($this->vhostMissing) {
            // Behind AMQProxy a missing vhost shows up only when the proxy opens the channel upstream.
            $this->connected = false;

            throw new AMQPConnectionClosedException('NOT_ALLOWED - vhost '.self::VHOST.' not found(0, 0)', 403);
        }

        $generation = $this->generation;
        $id = ++$this->nextChannelId;
        $alive = fn (): bool => $this->connected && ($generation === $this->generation || isset($this->staleWritable[$id]));
        $deliver = function (AMQPMessage $message, string $exchange = '', string $routingKey = '') use ($alive, $id): void {
            if (! $alive()) {
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
        $batch = [];

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
            if ($this->confirmSelectTimesOut) {
                throw new AMQPTimeoutException('The connection timed out after 3 sec while awaiting incoming data');
            }
            $this->confirmSelected[] = $generation;
        });
        $channel->shouldReceive('close')->andReturnUsing(function (): void {
            $this->channelsClosed++;
        });
        // Detached when its connection closed or reconnected; still attached after it reconnected on its own.
        $channel->shouldReceive('getConnection')->andReturnUsing(
            fn () => ($generation === $this->generation || isset($this->staleWritable[$id])) ? $this->connection : null
        );
        $channel->shouldReceive('getChannelId')->andReturn($id);
        $channel->shouldReceive('basic_get')->andReturnUsing(function (string $queue) {
            if ($this->queueMissingOnGet) {
                $this->queueMissingOnGet = false;

                throw new AMQPProtocolChannelException(404, "NOT_FOUND - no queue '$queue'", [60, 70]);
            }

            return null;
        });
        $channel->shouldReceive('basic_publish')->andReturnUsing($deliver);
        // A batch is only buffered on the channel; publish_batch writes it.
        $channel->shouldReceive('batch_basic_publish')->andReturnUsing(function (AMQPMessage $message, string $exchange = '', string $routingKey = '') use (&$batch): void {
            $batch[] = [$message, $exchange, $routingKey];
        });
        $channel->shouldReceive('publish_batch')->andReturnUsing(function () use (&$batch, $deliver): void {
            if ($this->publishBatchTimesOut) {
                // php-amqplib empties the batch only after a successful write.
                $this->publishBatchTimesOut = false;

                throw new AMQPTimeoutException('The connection timed out after 3 sec while sending data');
            }
            $messages = $batch;
            $batch = [];
            foreach ($messages as [$message, $exchange, $routingKey]) {
                $deliver($message, $exchange, $routingKey);
            }
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

    /**
     * After a missed heartbeat php-amqplib marks the connection closed without detaching its channels; the next
     * use connects again and empties the channel list, while the old channel objects still write on it.
     */
    private function reconnectOnItsOwnAfterAMissedHeartbeat(): void
    {
        foreach (array_keys($this->connection->channels) as $id) {
            $this->staleWritable[$id] = true;
        }
        $this->connection->channels = [];
        $this->generation++;
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
