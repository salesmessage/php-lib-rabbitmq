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
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use RuntimeException;
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
