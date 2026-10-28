<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Queue;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Events\CallQueuedListener;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery\MockInterface;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Dto\QueueApiDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Queue\QueueConfigFactory;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueueBatchable;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Lock\LockService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class RabbitMQQueueBatchableIndexationTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const LISTENER = 'App\\Listeners\\SendHubSpotTimelineEvent';

    private const QUEUE = 'hubspot-listener';

    private const QUORUM_QUEUE = 'hubspot-listener-quorum';

    private const VHOST = 'organization_1';

    private InternalStorageManager&MockInterface $storage;

    private LoggerInterface&MockInterface $logger;

    private Container $container;

    /** @var list<string> routing keys of the published messages */
    private array $published = [];

    /** @var list<string> vhost/queue of every queue indexed */
    private array $indexedQueues = [];

    /** @var list<string> every vhost indexed */
    private array $indexedVhosts = [];

    /** @var list<callable> */
    private array $afterCommitCallbacks = [];

    private ?\Throwable $indexingFailure = null;

    protected function setUp(): void
    {
        parent::setUp();

        $this->storage = Mockery::mock(InternalStorageManager::class);
        $this->storage->shouldReceive('setConnection')->with('rabbitmq_cluster')->andReturnSelf();
        $this->storage->shouldReceive('activateQueue')->andReturnUsing(function (QueueApiDto $queueDto): bool {
            if ($this->indexingFailure !== null) {
                throw $this->indexingFailure;
            }

            $this->indexedQueues[] = $queueDto->getVhostName().'/'.$queueDto->getName();

            return true;
        });
        $this->storage->shouldReceive('activateVhost')->andReturnUsing(function (VhostApiDto $vhostDto): bool {
            $this->indexedVhosts[] = $vhostDto->getName();

            return true;
        });

        $groupsService = Mockery::mock(GroupsService::class);
        $groupsService->shouldReceive('setConnection')->andReturnSelf();
        $groupsService->shouldReceive('getAllGroupsNames')->andReturn(['default']);

        $this->logger = Mockery::spy(LoggerInterface::class);

        $this->container = new Container;
        $this->container->instance(InternalStorageManager::class, $this->storage);
        $this->container->instance(GroupsService::class, $groupsService);
        $this->container->instance(VhostsService::class, Mockery::mock(VhostsService::class));
        $this->container->instance(LockService::class, Mockery::mock(LockService::class));
        $this->container->instance(LoggerInterface::class, $this->logger);
        Container::setInstance($this->container);

        $this->givenImmediateIndexation(true);
    }

    protected function tearDown(): void
    {
        Container::setInstance(null);

        parent::tearDown();
    }

    public function test_push_indexes_the_quorum_queue_it_published_to(): void
    {
        $this->makeQueue()->push(self::listenerJob(), '', self::QUEUE);

        $this->assertSame([self::QUORUM_QUEUE], $this->published);
        $this->assertSame([self::VHOST.'/'.self::QUORUM_QUEUE], $this->indexedQueues);
        $this->assertSame([self::VHOST], $this->indexedVhosts);
    }

    public function test_bulk_indexes_its_queue_once(): void
    {
        $this->makeQueue()->bulk([self::listenerJob(), self::listenerJob(), self::listenerJob()], '', self::QUEUE);

        $this->assertSame([self::QUORUM_QUEUE, self::QUORUM_QUEUE, self::QUORUM_QUEUE], $this->published);
        $this->assertSame([self::VHOST.'/'.self::QUORUM_QUEUE], $this->indexedQueues);
        $this->assertSame([self::VHOST], $this->indexedVhosts);
    }

    public function test_push_raw_indexes_the_queue_it_published_to(): void
    {
        $this->makeQueue()->pushRaw(self::payload(), self::QUORUM_QUEUE);

        $this->assertSame([self::QUORUM_QUEUE], $this->published);
        $this->assertSame([self::VHOST.'/'.self::QUORUM_QUEUE], $this->indexedQueues);
    }

    public function test_later_without_a_delay_indexes_the_queue(): void
    {
        $this->makeQueue()->later(0, self::listenerJob(), '', self::QUEUE);

        $this->assertSame([self::QUORUM_QUEUE], $this->published);
        $this->assertSame([self::VHOST.'/'.self::QUORUM_QUEUE], $this->indexedQueues);
    }

    public function test_later_with_a_delay_does_not_index_the_queue_the_message_is_not_in_yet(): void
    {
        $this->makeQueue()->later(30, self::listenerJob(), '', self::QUEUE);

        $this->assertSame([self::QUORUM_QUEUE], $this->published);
        $this->assertSame([], $this->indexedQueues);
        $this->assertSame([], $this->indexedVhosts);
    }

    public function test_push_after_commit_indexes_the_queue_only_once_the_message_is_published(): void
    {
        $this->container->instance('db.transactions', new class(function (callable $callback): void {
            $this->afterCommitCallbacks[] = $callback;
        }) {

            public function __construct(private \Closure $addCallback) {}

            public function addCallback(callable $callback): void
            {
                ($this->addCallback)($callback);
            }
        });

        $this->makeQueue(['after_commit' => true])->push(self::listenerJob(), '', self::QUEUE);

        $this->assertSame([], $this->published);
        $this->assertSame([], $this->indexedQueues);

        foreach ($this->afterCommitCallbacks as $callback) {
            $callback();
        }

        $this->assertSame([self::QUORUM_QUEUE], $this->published);
        $this->assertSame([self::VHOST.'/'.self::QUORUM_QUEUE], $this->indexedQueues);
    }

    public function test_publish_to_an_exchange_does_not_index_a_queue(): void
    {
        $this->makeQueue(['options' => ['queue' => ['exchange' => 'events']]])->push(self::listenerJob(), '', self::QUEUE);

        $this->assertSame([self::QUORUM_QUEUE], $this->published);
        $this->assertSame([], $this->indexedQueues);
    }

    public function test_nothing_is_indexed_when_immediate_indexation_is_off(): void
    {
        $this->givenImmediateIndexation(false);
        $queue = $this->makeQueue();

        $queue->push(self::listenerJob(), '', self::QUEUE);
        $queue->bulk([self::listenerJob()], '', self::QUEUE);
        $queue->pushRaw(self::payload(), self::QUORUM_QUEUE);

        $this->assertCount(3, $this->published);
        $this->assertSame([], $this->indexedQueues);
        $this->assertSame([], $this->indexedVhosts);
    }

    public function test_failed_indexing_does_not_fail_the_publish(): void
    {
        $this->indexingFailure = new \RuntimeException('Connection refused');

        $correlationId = $this->makeQueue()->push(self::listenerJob(), '', self::QUEUE);

        $this->assertNotNull($correlationId);
        $this->assertSame([self::QUORUM_QUEUE], $this->published);
        $this->logger->shouldHaveReceived('warning')->once()->with(
            'RabbitMQQueueBatchable.addQueueToIndex.exception',
            Mockery::subset([
                'vhost_name' => self::VHOST,
                'queue' => self::QUORUM_QUEUE,
                'error' => 'Connection refused',
            ])
        );
    }

    private function givenImmediateIndexation(bool $isEnabled): void
    {
        $this->container->instance('config', new Repository([
            'queue' => ['connections' => ['rabbitmq_cluster' => ['immediate_indexation' => $isEnabled]]],
        ]));
    }

    private function makeQueue(array $config = []): RabbitMQQueueBatchable
    {
        $queue = new RabbitMQQueueBatchable(QueueConfigFactory::make(array_merge_recursive([
            'options' => ['queue' => ['quorum_queue_postfix' => '-quorum']],
        ], $config)));
        $queue->setContainer($this->container);
        $queue->setConnectionName('rabbitmq_cluster:vhost:'.self::VHOST);
        $queue->setConnection($this->fakeConnection());

        return $queue;
    }

    private static function listenerJob(): CallQueuedListener
    {
        return new CallQueuedListener(self::LISTENER, 'handle', []);
    }

    private static function payload(): string
    {
        return json_encode(['displayName' => self::LISTENER, 'id' => 'job-id']);
    }

    private function fakeConnection(): AbstractConnection
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('isConnected')->andReturnTrue();
        $connection->shouldReceive('getHeartbeat')->andReturn(0);
        $connection->shouldReceive('getLastActivity')->andReturn(microtime(true));
        $connection->shouldReceive('channel')->andReturnUsing(function (): AMQPChannel {
            $record = function ($message, string $exchange, string $routingKey): void {
                $this->published[] = $routingKey;
            };

            $channel = Mockery::mock(AMQPChannel::class)->shouldIgnoreMissing();
            $channel->shouldReceive('queue_declare')->andReturnUsing(fn (string $name): array => [$name, 0, 0]);
            $channel->shouldReceive('basic_publish')->andReturnUsing($record);
            $channel->shouldReceive('batch_basic_publish')->andReturnUsing($record);

            return $channel;
        });

        return $connection;
    }
}
