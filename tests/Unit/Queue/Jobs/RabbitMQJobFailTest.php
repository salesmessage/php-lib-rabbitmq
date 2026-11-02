<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Queue\Jobs;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher as DispatcherContract;
use Illuminate\Database\Connection;
use Illuminate\Database\DatabaseManager;
use Illuminate\Events\Dispatcher;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\TimeoutExceededException;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery\MockInterface;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use RuntimeException;
use Salesmessage\LibRabbitMQ\Horizon\RabbitMQQueue as HorizonRabbitMQQueue;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;
use Salesmessage\LibRabbitMQ\Services\FailedJobWriter;

class RabbitMQJobFailTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private Container $container;

    private Dispatcher $events;

    private FailedJobProviderInterface&MockInterface $failer;

    private ExceptionHandler&MockInterface $exceptionHandler;

    /** @var list<string> */
    private array $calls = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->container = new Container;
        $this->events = new Dispatcher($this->container);
        $this->container->instance(DispatcherContract::class, $this->events);
        $this->container->instance('config', new Repository([
            'queue' => ['failed' => ['driver' => 'database', 'database' => 'pgsql', 'table' => 'failed_jobs']],
        ]));
        $this->failer = Mockery::mock(FailedJobProviderInterface::class);
        $this->container->instance(FailedJobWriter::class, new FailedJobWriter($this->failer, new NullLogger));
        $this->exceptionHandler = Mockery::mock(ExceptionHandler::class);
        $this->container->instance(ExceptionHandler::class, $this->exceptionHandler);
    }

    public function test_message_is_rejected_after_the_failed_job_row_is_written(): void
    {
        $this->events->listen(JobFailed::class, function (JobFailed $event) {
            $this->calls[] = 'logged';
            $event->job->markFailedJobLogged();
        });
        $this->failer->shouldNotReceive('log');
        $job = $this->makeJob($this->expectReject(false));

        $job->fail(new RuntimeException('job failed'));

        $this->assertSame(['logged', 'rejected'], $this->calls);
        $this->assertFalse($job->isRequeuedAfterFail());
    }

    public function test_job_writes_the_row_and_reports_the_exception_when_a_listener_throws_before_the_row_is_written(): void
    {
        $listenerException = new RuntimeException('listener broke');
        $this->events->listen(JobFailed::class, function () use ($listenerException) {
            throw $listenerException;
        });
        $jobException = new RuntimeException('job failed');
        $this->failer->shouldReceive('log')->once()
            ->with('rabbitmq_vhosts', 'default-quorum', $this->payload(), $jobException)
            ->andReturnUsing(function () {
                $this->calls[] = 'logged';

                return 1;
            });
        $this->exceptionHandler->shouldReceive('report')->once()->with($listenerException);
        $job = $this->makeJob($this->expectReject(false));

        $job->fail($jobException);

        $this->assertSame(['logged', 'rejected'], $this->calls);
        $this->assertFalse($job->isRequeuedAfterFail());
    }

    public function test_message_is_requeued_and_the_exception_reported_when_the_row_is_not_written(): void
    {
        $listenerException = new RuntimeException('database is down');
        $this->events->listen(JobFailed::class, function () use ($listenerException) {
            throw $listenerException;
        });
        $this->failer->shouldReceive('log')->once()->andThrow(new RuntimeException('database is still down'));
        $this->exceptionHandler->shouldReceive('report')->once()->with($listenerException);
        $job = $this->makeJob($this->expectReject(true));

        $job->fail(new RuntimeException('job failed'));

        $this->assertTrue($job->isRequeuedAfterFail());
    }

    public function test_message_is_rejected_and_the_exception_reported_when_the_row_is_written_but_the_failed_callback_throws(): void
    {
        $this->container->instance('FailingHandler', new class
        {
            public function failed(): void
            {
                throw new RuntimeException('failed callback broke');
            }
        });
        $this->events->listen(JobFailed::class, fn (JobFailed $event) => $event->job->markFailedJobLogged());
        $this->exceptionHandler->shouldReceive('report')->once()
            ->with(Mockery::on(fn (RuntimeException $exception) => $exception->getMessage() === 'failed callback broke'));

        $this->makeJob($this->expectReject(false), 'FailingHandler@call')->fail(new RuntimeException('job failed'));
    }

    public function test_failing_the_same_job_twice_rejects_its_message_once(): void
    {
        $this->events->listen(JobFailed::class, function (JobFailed $event) {
            $this->calls[] = 'logged';
            $event->job->markFailedJobLogged();
        });
        $job = $this->makeJob($this->expectReject(false));

        $job->fail(new TimeoutExceededException('job timed out'));
        $job->fail(new TimeoutExceededException('job timed out'));

        $this->assertSame(['logged', 'rejected'], $this->calls);
    }

    public function test_requeued_message_is_not_rejected_when_the_job_fails_again(): void
    {
        $this->events->listen(JobFailed::class, function () {
            throw new RuntimeException('database is down');
        });
        $this->failer->shouldReceive('log')->once()->andThrow(new RuntimeException('database is still down'));
        $this->exceptionHandler->shouldReceive('report')->once();
        $job = $this->makeJob($this->expectReject(true));

        $job->fail(new TimeoutExceededException('job timed out'));
        $job->fail(new TimeoutExceededException('job timed out'));

        $this->assertTrue($job->isRequeuedAfterFail());
    }

    public function test_message_of_a_job_deleted_before_it_failed_is_not_rejected(): void
    {
        $this->failer->shouldNotReceive('log');
        $rabbitmq = Mockery::mock(RabbitMQQueue::class);
        $rabbitmq->shouldReceive('ack')->once();
        $rabbitmq->shouldNotReceive('reject');
        $job = $this->makeJob($rabbitmq);
        $job->delete();

        $job->fail(new RuntimeException('job failed'));

        $this->assertTrue($job->hasFailed());
    }

    public function test_timed_out_job_rolls_back_the_open_transaction_before_the_row_is_written(): void
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('rollBack')->once()->with(0)->andReturnUsing(function () {
            $this->calls[] = 'rolled back';
        });
        $db = Mockery::mock(DatabaseManager::class);
        $db->shouldReceive('connection')->with('pgsql')->andReturn($connection);
        $this->container->instance('db', $db);
        $this->events->listen(JobFailed::class, function (JobFailed $event) {
            $this->calls[] = 'logged';
            $event->job->markFailedJobLogged();
        });

        $this->makeJob($this->expectReject(false))->fail(new TimeoutExceededException('job timed out'));

        $this->assertSame(['rolled back', 'logged', 'rejected'], $this->calls);
    }

    public function test_failed_write_of_the_reject_is_reported_instead_of_thrown(): void
    {
        $this->events->listen(JobFailed::class, function (JobFailed $event) {
            $event->job->markFailedJobLogged();
        });
        $writeError = new AMQPRuntimeException('Error sending data');
        $rabbitmq = Mockery::mock(RabbitMQQueue::class);
        $rabbitmq->shouldReceive('reject')->once()->andThrow($writeError);
        $this->exceptionHandler->shouldReceive('report')->once()->with($writeError);

        $this->makeJob($rabbitmq)->fail(new RuntimeException('job timed out'));
    }

    public function test_closed_delivery_channel_on_the_reject_is_reported_instead_of_thrown(): void
    {
        // The reject goes to the channel that delivered the message; when it is gone, the timeout handler must
        // still get to kill the worker. The broker requeued the message when the channel closed.
        $this->events->listen(JobFailed::class, function (JobFailed $event) {
            $event->job->markFailedJobLogged();
        });
        $closed = new AMQPChannelClosedException('Channel connection is closed.');
        $rabbitmq = Mockery::mock(RabbitMQQueue::class);
        $rabbitmq->shouldReceive('reject')->once()->andThrow($closed);
        $this->exceptionHandler->shouldReceive('report')->once()->with($closed);

        $this->makeJob($rabbitmq)->fail(new RuntimeException('job timed out'));
    }

    public function test_closed_delivery_channel_on_the_requeue_is_reported_with_the_listener_exception(): void
    {
        $listenerException = new RuntimeException('listener broke');
        $this->events->listen(JobFailed::class, function () use ($listenerException) {
            throw $listenerException;
        });
        $this->failer->shouldReceive('log')->andThrow(new RuntimeException('database down'));
        $closed = new AMQPChannelClosedException('Channel connection is closed.');
        $rabbitmq = Mockery::mock(RabbitMQQueue::class);
        $rabbitmq->shouldReceive('reject')->once()->andThrow($closed);
        $this->exceptionHandler->shouldReceive('report')->once()->with($closed);
        $this->exceptionHandler->shouldReceive('report')->once()->with($listenerException);

        $this->makeJob($rabbitmq)->fail(new RuntimeException('job failed'));
    }

    public function test_horizon_job_is_no_longer_reserved_when_its_ack_fails(): void
    {
        $closed = new AMQPChannelClosedException('Channel connection is closed.');
        $rabbitmq = $this->withConnection(Mockery::mock(HorizonRabbitMQQueue::class), connected: true);
        $rabbitmq->shouldReceive('ack')->once()->andThrow($closed);
        $rabbitmq->shouldReceive('deleteReserved')->once();
        $this->exceptionHandler->shouldReceive('report')->once()->with($closed);

        $this->makeJob($rabbitmq)->delete();
    }

    public function test_finished_job_whose_delivery_channel_is_gone_is_reported_not_failed(): void
    {
        // A reconnect while the job ran (e.g. its own publish) closed the channel that delivered it: the broker
        // requeued the message, and the job, which succeeded, must not fail or stop the worker.
        $closed = new AMQPChannelClosedException('Channel connection is closed.');
        $rabbitmq = $this->withConnection(Mockery::mock(RabbitMQQueue::class), connected: true);
        $rabbitmq->shouldReceive('ack')->once()->andThrow($closed);
        $this->exceptionHandler->shouldReceive('report')->once()->with($closed);
        $job = $this->makeJob($rabbitmq);

        $job->delete();

        $this->assertTrue($job->isDeleted());
    }

    public function test_finished_job_on_a_lost_connection_lets_the_worker_restart(): void
    {
        // The connection itself is down (no reconnect happened): the error reaches Laravel's worker, which
        // restarts on a lost connection.
        $closed = new AMQPChannelClosedException('Channel connection is closed.');
        $rabbitmq = $this->withConnection(Mockery::mock(RabbitMQQueue::class), connected: false);
        $rabbitmq->shouldReceive('ack')->once()->andThrow($closed);
        $this->exceptionHandler->shouldNotReceive('report');

        $this->expectExceptionObject($closed);
        $this->makeJob($rabbitmq)->delete();
    }

    public function test_finished_job_is_reported_when_its_delivery_connection_is_up_whatever_the_queue_connection(): void
    {
        // A consumer may deliver through another queue object's connection: the delivery channel's decides.
        $closed = new AMQPChannelClosedException('Channel connection is closed.');
        $rabbitmq = $this->withConnection(Mockery::mock(RabbitMQQueue::class), connected: false);
        $rabbitmq->shouldReceive('ack')->once()->andThrow($closed);
        $this->exceptionHandler->shouldReceive('report')->once()->with($closed);
        $deliveryConnection = Mockery::mock(AbstractConnection::class);
        $deliveryConnection->shouldReceive('isConnected')->andReturnTrue();
        $deliveryChannel = Mockery::mock(AMQPChannel::class);
        $deliveryChannel->shouldReceive('getConnection')->andReturn($deliveryConnection);
        $message = new AMQPMessage($this->payload());
        $message->setChannel($deliveryChannel);
        $message->setDeliveryInfo(5, false, '', 'default-quorum');

        (new RabbitMQJob($this->container, $rabbitmq, $message, 'rabbitmq_vhosts', 'default-quorum'))->delete();
    }

    public function test_write_timeout_of_the_reject_is_reported_instead_of_thrown(): void
    {
        $this->events->listen(JobFailed::class, function (JobFailed $event) {
            $event->job->markFailedJobLogged();
        });
        $timeout = new AMQPTimeoutException('The connection timed out after 3 sec while sending data');
        $rabbitmq = Mockery::mock(RabbitMQQueue::class);
        $rabbitmq->shouldReceive('reject')->once()->andThrow($timeout);
        $this->exceptionHandler->shouldReceive('report')->once()->with($timeout);

        $this->makeJob($rabbitmq)->fail(new RuntimeException('job timed out'));
    }

    public function test_released_job_whose_delivery_channel_is_gone_is_republished_and_reported(): void
    {
        $closed = new AMQPChannelClosedException('Channel connection is closed.');
        $rabbitmq = $this->withConnection(Mockery::mock(RabbitMQQueue::class), connected: true);
        $rabbitmq->shouldReceive('laterRaw')->once()->andReturn('id');
        $rabbitmq->shouldReceive('ack')->once()->andThrow($closed);
        $this->exceptionHandler->shouldReceive('report')->once()->with($closed);
        $message = new AMQPMessage(json_encode([
            'uuid' => '0b6a0d5c-2a8e-4f6e-9a39-3f8d1f7b9a10',
            'job' => 'stdClass@call',
            'data' => ['commandName' => 'stdClass', 'command' => serialize(new \stdClass)],
        ]));
        $message->setDeliveryInfo(5, false, '', 'default-quorum');
        $job = new RabbitMQJob($this->container, $rabbitmq, $message, 'rabbitmq_vhosts', 'default-quorum');

        $job->release(10);

        $this->assertTrue($job->isReleased());
    }

    private function withConnection(RabbitMQQueue&MockInterface $rabbitmq, bool $connected): RabbitMQQueue&MockInterface
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('isConnected')->andReturn($connected);
        $rabbitmq->shouldReceive('getConnection')->andReturn($connection);

        return $rabbitmq;
    }

    private function expectReject(bool $requeue): RabbitMQQueue
    {
        $rabbitmq = Mockery::mock(RabbitMQQueue::class);
        $rabbitmq->shouldReceive('reject')->once()
            ->with(Mockery::type(RabbitMQJob::class), $requeue)
            ->andReturnUsing(function () {
                $this->calls[] = 'rejected';
            });
        $rabbitmq->shouldNotReceive('ack');

        return $rabbitmq;
    }

    private function makeJob(RabbitMQQueue $rabbitmq, string $handler = 'stdClass@call'): RabbitMQJob
    {
        $message = new AMQPMessage($this->payload($handler));
        $message->setDeliveryInfo(5, false, '', 'default-quorum');

        return new RabbitMQJob($this->container, $rabbitmq, $message, 'rabbitmq_vhosts', 'default-quorum');
    }

    private function payload(string $handler = 'stdClass@call'): string
    {
        return json_encode([
            'uuid' => '0b6a0d5c-2a8e-4f6e-9a39-3f8d1f7b9a10',
            'job' => $handler,
            'data' => ['commandName' => 'stdClass', 'command' => ''],
        ]);
    }
}
