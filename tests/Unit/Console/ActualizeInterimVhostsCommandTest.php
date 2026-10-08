<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Console;

use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Salesmessage\LibRabbitMQ\Dto\InterimVhostsDto;
use Salesmessage\LibRabbitMQ\Dto\VhostApiDto;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;
use Salesmessage\LibRabbitMQ\Services\InterimVhosts\InterimVhostsSourceFactory;
use Salesmessage\LibRabbitMQ\Services\InterimVhosts\InterimVhostsSourceInterface;
use Salesmessage\LibRabbitMQ\Tests\Support\RedisBackedTestCase;

class ActualizeInterimVhostsCommandTest extends RedisBackedTestCase
{
    use MockeryPHPUnitIntegration;

    private const INTERIM_KEY = 'rabbitmq_interim_vhosts';

    protected function setUp(): void
    {
        parent::setUp();

        $this->app->instance(LoggerInterface::class, new NullLogger);
    }

    public function test_writes_vhosts_and_removes_ones_no_longer_reported(): void
    {
        $this->redis->hset(self::INTERIM_KEY, 'org_gone', '{"name":"org_gone"}');
        $this->redis->hset(self::INTERIM_KEY, 'org_1', '{"name":"org_1","messages":0}');

        $source = Mockery::mock(InterimVhostsSourceInterface::class);
        $source->shouldReceive('getVhosts')->once()->andReturn(new InterimVhostsDto([
            new VhostApiDto(['name' => 'org_1', 'messages' => 3, 'messages_ready' => 3]),
            new VhostApiDto(['name' => 'org_2']),
        ]));
        $this->bindSource($source);

        $this->artisan('lib-rabbitmq:actualize-interim-vhosts', ['--sleep' => 0])->assertExitCode(0);

        $this->assertEqualsCanonicalizing(['org_1', 'org_2'], array_keys($this->redis->hgetall(self::INTERIM_KEY)));
        $this->assertSame(
            ['name' => 'org_1', 'messages' => 3, 'messages_ready' => 3, 'messages_unacknowledged' => 0],
            json_decode($this->redis->hget(self::INTERIM_KEY, 'org_1'), true)
        );
    }

    public function test_failed_read_leaves_interim_vhosts_unchanged(): void
    {
        $this->redis->hset(self::INTERIM_KEY, 'org_1', '{"name":"org_1","messages":5}');
        $before = $this->redis->hgetall(self::INTERIM_KEY);

        $source = Mockery::mock(InterimVhostsSourceInterface::class);
        $source->shouldReceive('getVhosts')->once()->andThrow(new PrometheusMetricsException('node down'));
        $this->bindSource($source);

        $this->artisan('lib-rabbitmq:actualize-interim-vhosts', ['--sleep' => 0])->assertExitCode(0);

        $this->assertSame($before, $this->redis->hgetall(self::INTERIM_KEY));
    }

    public function test_vhost_with_uncounted_queues_keeps_its_interim_data(): void
    {
        $this->redis->hset(self::INTERIM_KEY, 'org_1', '{"name":"org_1","messages":5}');
        $this->redis->hset(self::INTERIM_KEY, 'org_gone', '{"name":"org_gone"}');

        $source = Mockery::mock(InterimVhostsSourceInterface::class);
        $source->shouldReceive('getVhosts')->once()->andReturn(new InterimVhostsDto(
            [new VhostApiDto(['name' => 'org_2', 'messages' => 1, 'messages_ready' => 1])],
            ['org_1' => ['q1']]
        ));
        $this->bindSource($source);

        $this->artisan('lib-rabbitmq:actualize-interim-vhosts', ['--sleep' => 0])->assertExitCode(0);

        $this->assertEqualsCanonicalizing(['org_1', 'org_2'], array_keys($this->redis->hgetall(self::INTERIM_KEY)));
        $this->assertSame('{"name":"org_1","messages":5}', $this->redis->hget(self::INTERIM_KEY, 'org_1'));
    }

    public function test_empty_result_leaves_interim_vhosts_unchanged(): void
    {
        $this->redis->hset(self::INTERIM_KEY, 'org_1', '{"name":"org_1","messages":5}');
        $before = $this->redis->hgetall(self::INTERIM_KEY);

        $source = Mockery::mock(InterimVhostsSourceInterface::class);
        $source->shouldReceive('getVhosts')->once()->andReturn(new InterimVhostsDto([]));
        $this->bindSource($source);

        $this->artisan('lib-rabbitmq:actualize-interim-vhosts', ['--sleep' => 0])->assertExitCode(0);

        $this->assertSame($before, $this->redis->hgetall(self::INTERIM_KEY));
    }

    private function bindSource(InterimVhostsSourceInterface $source): void
    {
        $factory = Mockery::mock(InterimVhostsSourceFactory::class);
        $factory->shouldReceive('make')->with('rabbitmq_vhosts')->andReturn($source);

        $this->app->instance(InterimVhostsSourceFactory::class, $factory);
    }
}
