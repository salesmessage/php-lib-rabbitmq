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
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
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

    private const VHOST = 'organization_1';

    private LoggerInterface&MockInterface $logger;

    private VhostsService&MockInterface $vhostsService;

    private LockService&MockInterface $lockService;

    private RabbitMQQueueBatchable $queue;

    private bool $connectionDead = false;

    private bool $vhostDeleted = false;

    private float $lastActivity;

    private int $generation = 1;

    private int $reconnects = 0;

    /**
     * The queue exists before the process publishes, as in production: a passive declare finds and caches it,
     * so later publishes do no I/O before the write.
     *
     * @var list<string>
     */
    private array $brokerQueues = [self::QUEUE];

    /** @var list<array{0: string, 1: string|null}> */
    private array $published = [];

    protected function setUp(): void
    {
        parent::setUp();

        Carbon::setTestNow(Carbon::parse('2026-10-20 12:00:00'));
        $this->lastActivity = $this->secondsAgo(1);

        $this->logger = Mockery::spy(LoggerInterface::class);
        $this->vhostsService = Mockery::mock(VhostsService::class);
        $this->lockService = Mockery::mock(LockService::class);

        $container = new Container;
        $container->instance('config', new Repository([]));
        $container->instance(InternalStorageManager::class, Mockery::mock(InternalStorageManager::class));
        $container->instance(GroupsService::class, Mockery::mock(GroupsService::class));
        $container->instance(VhostsService::class, $this->vhostsService);
        $container->instance(LockService::class, $this->lockService);
        $container->instance(LoggerInterface::class, $this->logger);
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
            'push' => [fn (RabbitMQQueueBatchable $queue) => $queue->push(self::listenerJob(), '', self::QUEUE)],
            'pushRaw' => [fn (RabbitMQQueueBatchable $queue) => $queue->pushRaw(
                json_encode(['displayName' => self::LISTENER, 'id' => 'job-id']),
                self::QUEUE
            )],
            'bulk' => [fn (RabbitMQQueueBatchable $queue) => $queue->bulk([self::listenerJob()], '', self::QUEUE)],
            'laterRaw with a delay' => [fn (RabbitMQQueueBatchable $queue) => $queue->laterRaw(
                30,
                json_encode(['displayName' => self::LISTENER, 'id' => 'job-id']),
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

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
        $this->logger->shouldHaveReceived('warning')->once()->with(
            'RabbitMQQueueBatchable.reconnect',
            Mockery::subset([
                'reason' => 'idle_guard',
                'vhost' => self::VHOST,
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

    #[DataProvider('publishPaths')]
    public function test_publish_after_the_vhost_of_an_idle_connection_was_deleted_declares_the_queue_in_the_recreated_vhost(callable $publish): void
    {
        $this->lockService->shouldReceive('lock')->andReturnUsing(function (string $key, \Closure $handler): bool {
            $handler();

            return true;
        });
        $this->vhostsService->shouldReceive('setConnection')->andReturnSelf();
        $this->vhostsService->shouldReceive('createVhost')->once()->with(self::VHOST, Mockery::any())
            ->andReturnUsing(function (): bool {
                $this->vhostDeleted = false;

                return true;
            });
        $publish($this->queue);
        $this->published = [];

        $this->lastActivity = $this->secondsAgo(45);
        $this->connectionDead = true;
        $this->vhostDeleted = true;
        $this->brokerQueues = [];
        $publish($this->queue);

        $this->assertSame([['delivered', self::LISTENER]], $this->published);
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
     * A publish counts as delivered only when it goes through a channel of the current, live connection to a
     * queue the broker has: a write into a connection the peer has closed returns normally and the message is
     * lost, and so is an unroutable message on a channel without a return listener.
     */
    private function fakeConnection(): AbstractConnection
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('isConnected')->andReturnTrue();
        $connection->shouldReceive('getHeartbeat')->andReturn(30);
        $connection->shouldReceive('getLastActivity')->andReturnUsing(fn () => $this->lastActivity);
        $connection->shouldReceive('reconnect')->andReturnUsing(function (): void {
            if ($this->vhostDeleted) {
                throw new AMQPConnectionClosedException('NOT_ALLOWED - vhost '.self::VHOST.' not found', 530);
            }

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
            throw new AMQPConnectionClosedException($this->vhostDeleted
                ? "CONNECTION_FORCED - vhost '".self::VHOST."' is deleted"
                : 'Broken pipe or closed connection');
        }

        $generation = $this->generation;
        $record = function (AMQPMessage $message, string $exchange, string $routingKey) use ($generation): void {
            $delivered = $generation === $this->generation
                && ! $this->connectionDead
                && in_array($routingKey, $this->brokerQueues, true);
            $this->published[] = [
                $delivered ? 'delivered' : 'lost',
                json_decode($message->getBody(), true)['displayName'] ?? null,
            ];
        };

        // Publisher confirms and the delay queue exchange/binding calls need no behaviour here.
        $channel = Mockery::mock(AMQPChannel::class)->shouldIgnoreMissing();
        $channel->shouldReceive('queue_declare')->andReturnUsing(function (string $name, bool $passive): array {
            if (! in_array($name, $this->brokerQueues, true)) {
                if ($passive) {
                    throw new AMQPProtocolChannelException(404, "NOT_FOUND - no queue '$name'", [50, 10]);
                }
                $this->brokerQueues[] = $name;
            }

            return [$name, 0, 0];
        });
        $channel->shouldReceive('close', 'publish_batch')->andReturnNull();
        $channel->shouldReceive('basic_publish')->andReturnUsing($record);
        $channel->shouldReceive('batch_basic_publish')->andReturnUsing($record);

        return $channel;
    }
}
