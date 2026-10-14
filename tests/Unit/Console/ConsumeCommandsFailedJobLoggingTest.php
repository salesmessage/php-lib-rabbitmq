<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Console;

use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Queue\Console\WorkCommand;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\Worker;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;
use Salesmessage\LibRabbitMQ\Console\ConsumeCommand;
use Salesmessage\LibRabbitMQ\Console\ConsumeVhostsCommand;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Tests\TestCase;

class ConsumeCommandsFailedJobLoggingTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const PAYLOAD = '{"uuid":"0b6a0d5c-2a8e-4f6e-9a39-3f8d1f7b9a10"}';

    /**
     * @return array<string, array{class-string<WorkCommand>}>
     */
    public static function commands(): array
    {
        return [
            'lib-rabbitmq:consume' => [ConsumeCommand::class],
            'lib-rabbitmq:consume-vhosts' => [ConsumeVhostsCommand::class],
        ];
    }

    #[DataProvider('commands')]
    public function test_job_is_marked_as_logged_once_the_row_is_written(string $commandClass): void
    {
        $exception = new RuntimeException('job failed');
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()
            ->with('rabbitmq_vhosts', 'default-quorum', self::PAYLOAD, $exception)
            ->andReturn(1);
        $job = $this->makeJob();
        $job->shouldReceive('markFailedJobLogged')->once();

        $this->logFailedJob($commandClass, $failer, new JobFailed('rabbitmq_vhosts', $job, $exception));
    }

    #[DataProvider('commands')]
    public function test_job_is_not_marked_as_logged_when_the_row_is_not_written(string $commandClass): void
    {
        $failer = Mockery::mock(FailedJobProviderInterface::class);
        $failer->shouldReceive('log')->once()->andThrow(new RuntimeException('database is down'));
        $job = $this->makeJob();
        $job->shouldNotReceive('markFailedJobLogged');

        $this->expectExceptionMessage('database is down');

        $this->logFailedJob($commandClass, $failer, new JobFailed('rabbitmq_vhosts', $job, new RuntimeException('job failed')));
    }

    private function makeJob(): RabbitMQJob
    {
        $job = Mockery::mock(RabbitMQJob::class);
        $job->shouldReceive('getQueue')->andReturn('default-quorum');
        $job->shouldReceive('getRawBody')->andReturn(self::PAYLOAD);

        return $job;
    }

    /**
     * @param  class-string<WorkCommand>  $commandClass
     */
    private function logFailedJob(string $commandClass, FailedJobProviderInterface $failer, JobFailed $event): void
    {
        $this->app->instance('queue.failer', $failer);

        $command = $commandClass === ConsumeVhostsCommand::class
            ? new ConsumeVhostsCommand(Mockery::mock(GroupsService::class), Mockery::mock(Worker::class), Mockery::mock(Cache::class))
            : new ConsumeCommand(Mockery::mock(Worker::class), Mockery::mock(Cache::class));
        $command->setLaravel($this->app);

        (fn () => $this->logFailedJob($event))->call($command);
    }
}
