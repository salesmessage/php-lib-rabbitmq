<?php

namespace Salesmessage\LibRabbitMQ\Tests\Feature;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use LogicException;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Queue\QueueConfigFactory;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueueBatchable;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\Lock\LockService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

/**
 * Publishing through AMQProxy whose broker is unreachable (the `amqproxy-upstream-down` service in
 * docker-compose.yml). The proxy completes the AMQP handshake itself and closes the connection on every
 * channel.open with UPSTREAM_ERROR, so a reconnect always succeeds and only the channel fails.
 */
class AmqproxyUpstreamDownTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    /** Far above the bounded retries; stops an unbounded recursion before it exhausts the memory. */
    private const MAX_RECONNECT_WARNINGS = 50;

    private ?AMQPStreamConnection $connection = null;

    protected function setUp(): void
    {
        parent::setUp();

        $host = getenv('AMQPROXY_HOST') ?: getenv('HOST');
        $port = (int) getenv('AMQPROXY_PORT');
        $socket = @fsockopen($host, $port, $errorCode, $errorMessage, 2);
        if ($socket === false) {
            $message = "AMQProxy is not running on $host:$port ($errorMessage): docker-compose up -d amqproxy-upstream-down.";
            // In CI the service is started before the tests, so a missing proxy is a failure, not a skip.
            getenv('CI') ? $this->fail($message) : $this->markTestSkipped($message);
        }
        fclose($socket);

        $this->connection = new AMQPStreamConnection($host, $port, 'guest', 'guest', '/', connection_timeout: 3, read_write_timeout: 3);
    }

    protected function tearDown(): void
    {
        Container::setInstance(null);

        parent::tearDown();
    }

    public function test_publishes_fail_with_a_closed_connection_in_bounded_time_without_memory_growth(): void
    {
        $logger = new class(self::MAX_RECONNECT_WARNINGS) extends AbstractLogger
        {
            public int $reconnects = 0;

            public function __construct(private readonly int $max) {}

            public function log($level, string|\Stringable $message, array $context = []): void
            {
                if ((string) $message === 'RabbitMQQueueBatchable.reconnect' && ++$this->reconnects > $this->max) {
                    throw new LogicException("Unbounded reconnect recursion: {$this->max} reconnect warnings.");
                }
            }
        };

        $container = new Container;
        $container->instance('config', new Repository([]));
        $container->instance(InternalStorageManager::class, Mockery::mock(InternalStorageManager::class)->shouldIgnoreMissing());
        $container->instance(GroupsService::class, Mockery::mock(GroupsService::class));
        $container->instance(VhostsService::class, Mockery::mock(VhostsService::class));
        $container->instance(LockService::class, Mockery::mock(LockService::class));
        $container->instance(LoggerInterface::class, $logger);
        Container::setInstance($container);

        $queue = new RabbitMQQueueBatchable(QueueConfigFactory::make([]));
        $queue->setContainer($container);
        $queue->setConnectionName('rabbitmq:vhost:/');
        $queue->setConnection($this->connection);

        // The worker keeps running: every publish fails the same way, in bounded time, and the memory does not
        // grow from one failed publish to the next (the first one also loads the classes it needs).
        $memoryAfterFirst = null;
        for ($publish = 1; $publish <= 3; $publish++) {
            $logger->reconnects = 0;
            $start = microtime(true);
            try {
                $queue->pushRaw(json_encode(['displayName' => 'AmqproxyUpstreamDownJob', 'id' => "job-$publish"]), 'amqproxy-upstream-down');
                $this->fail('Publishing with the broker down must throw.');
            } catch (AMQPConnectionClosedException $exception) {
                $this->assertStringContainsString('UPSTREAM_ERROR', $exception->getMessage());
            }
            unset($exception);

            // The attempts after the first wait 0.2 + 0.4 + 0.8 s for the broker to come back.
            $elapsed = microtime(true) - $start;
            $this->assertGreaterThan(1.2, $elapsed);
            $this->assertLessThan(10, $elapsed);
            $this->assertLessThanOrEqual(4, $logger->reconnects);
            $memoryAfterFirst ??= memory_get_usage();
        }

        $this->assertLessThan(1024 * 1024, memory_get_usage() - $memoryAfterFirst);
    }
}
