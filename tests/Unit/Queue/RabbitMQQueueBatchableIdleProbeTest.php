<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Queue;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Events\CallQueuedListener;
use Illuminate\Support\Carbon;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery\MockInterface;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Queue\QueueConfigFactory;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueueBatchable;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Lock\LockService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class RabbitMQQueueBatchableIdleProbeTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const LISTENER = 'App\\Listeners\\SendHubSpotTimelineEvent';

    private const QUEUE = 'hubspot-listener';

    private LoggerInterface&MockInterface $logger;

    private RabbitMQQueueBatchable $queue;

    private bool $connectionDead = false;

    private float $lastActivity;

    private int $heartbeat = 30;

    private int $generation = 1;

    private int $reconnects = 0;

    private int $channelsOpened = 0;

    /** @var list<array{0: string, 1: string|null}> */
    private array $published = [];

    protected function setUp(): void
    {
        parent::setUp();

        Carbon::setTestNow(Carbon::parse('2026-10-20 12:00:00'));
        $this->lastActivity = $this->secondsAgo(1);

        $this->logger = Mockery::spy(LoggerInterface::class);

        $container = new Container;
        $container->instance('config', new Repository([]));
        $container->instance(InternalStorageManager::class, Mockery::mock(InternalStorageManager::class));
        $container->instance(GroupsService::class, Mockery::mock(GroupsService::class));
        $container->instance(VhostsService::class, Mockery::mock(VhostsService::class));
        $container->instance(LockService::class, Mockery::mock(LockService::class));
        $container->instance(LoggerInterface::class, $this->logger);
        Container::setInstance($container);

        $this->queue = new RabbitMQQueueBatchable(QueueConfigFactory::make([]));
        $this->queue->setContainer($container);
        $this->queue->setConnectionName('rabbitmq_cluster:vhost:organization_1');
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
            'push' => [fn (RabbitMQQueueBatchable $queue) => $queue->push(self::listenerJob(), '', self::QUEUE)],
            'pushRaw' => [fn (RabbitMQQueueBatchable $queue) => $queue->pushRaw(
                json_encode(['displayName' => self::LISTENER, 'id' => 'job-id']),
                self::QUEUE
            )],
            'bulk' => [fn (RabbitMQQueueBatchable $queue) => $queue->bulk(
                [self::listenerJob(), self::listenerJob()],
                '',
                self::QUEUE
            )],
        ];
    }

    #[DataProvider('publishPaths')]
    public function test_publish_after_the_proxy_closed_an_idle_connection_goes_out_on_a_new_connection(callable $publish): void
    {
        $publish($this->queue);
        $this->published = [];

        $this->lastActivity = $this->secondsAgo(45);
        $this->connectionDead = true;
        $publish($this->queue);

        $this->assertNotEmpty($this->published);
        $this->assertSame(['delivered'], array_values(array_unique(array_column($this->published, 0))));
        $this->assertSame(1, $this->reconnects);
        $this->logger->shouldHaveReceived('warning')->once()->with(
            'RabbitMQQueueBatchable.reconnect',
            Mockery::subset([
                'reason' => 'idle_guard',
                'vhost' => 'organization_1',
                'idle_seconds' => 45.0,
                'job' => self::LISTENER,
            ])
        );
    }

    public function test_alive_idle_connection_is_kept_so_a_consumer_sharing_it_keeps_its_channel(): void
    {
        $this->queue->push(self::listenerJob(), '', self::QUEUE);
        $this->published = [];

        $this->lastActivity = $this->secondsAgo(45);
        $this->queue->push(self::listenerJob(), '', self::QUEUE);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
        $this->assertSame(0, $this->reconnects);
        $this->logger->shouldNotHaveReceived('warning');
    }

    public static function connectionsWithoutProbe(): array
    {
        return [
            'active within the heartbeat interval' => [5.0, 30],
            'heartbeat disabled' => [45.0, 0],
        ];
    }

    #[DataProvider('connectionsWithoutProbe')]
    public function test_publish_adds_no_round_trip_when_the_connection_cannot_have_been_closed_for_idleness(
        float $idleSeconds,
        int $heartbeat
    ): void {
        $this->queue->push(self::listenerJob(), '', self::QUEUE);
        $channelsOpened = $this->channelsOpened;

        $this->heartbeat = $heartbeat;
        $this->lastActivity = $this->secondsAgo($idleSeconds);
        $this->queue->push(self::listenerJob(), '', self::QUEUE);

        $this->assertSame($channelsOpened, $this->channelsOpened);
        $this->assertSame(0, $this->reconnects);
    }

    private static function listenerJob(): CallQueuedListener
    {
        return new CallQueuedListener(self::LISTENER, 'handle', []);
    }

    private function secondsAgo(float $seconds): float
    {
        return Carbon::now()->getPreciseTimestamp() / 1_000_000 - $seconds;
    }

    /**
     * A publish counts as delivered only when it goes through a channel of the current, live connection:
     * a write into a connection the peer has closed returns normally and the message is lost.
     */
    private function fakeConnection(): AbstractConnection
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('isConnected')->andReturnTrue();
        $connection->shouldReceive('getHeartbeat')->andReturnUsing(fn () => $this->heartbeat);
        $connection->shouldReceive('getLastActivity')->andReturnUsing(fn () => $this->lastActivity);
        $connection->shouldReceive('reconnect')->andReturnUsing(function (): void {
            $this->connectionDead = false;
            $this->generation++;
            $this->reconnects++;
        });
        $connection->shouldReceive('channel')->andReturnUsing(fn () => $this->openChannel());

        return $connection;
    }

    private function openChannel(): AMQPChannel
    {
        if ($this->connectionDead) {
            throw new AMQPConnectionClosedException('Broken pipe or closed connection');
        }

        $this->channelsOpened++;
        $generation = $this->generation;
        $record = function (AMQPMessage $message) use ($generation): void {
            $outcome = ($generation === $this->generation && ! $this->connectionDead) ? 'delivered' : 'lost';
            $this->published[] = [$outcome, json_decode($message->getBody(), true)['displayName'] ?? null];
        };

        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('queue_declare', 'close', 'publish_batch')->andReturnNull();
        $channel->shouldReceive('basic_publish')->andReturnUsing($record);
        $channel->shouldReceive('batch_basic_publish')->andReturnUsing($record);

        return $channel;
    }
}
