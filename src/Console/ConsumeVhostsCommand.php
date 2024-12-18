<?php

namespace Salesmessage\LibRabbitMQ\Console;

use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Queue\Console\WorkCommand;
use Illuminate\Queue\Worker;
use Illuminate\Support\Str;
use Salesmessage\LibRabbitMQ\Dto\ConsumeVhostsFiltersDto;
use Symfony\Component\Console\Terminal;
use Salesmessage\LibRabbitMQ\VhostsConsumer;

class ConsumeVhostsCommand extends WorkCommand
{
    protected $signature = 'lib-rabbitmq:consume-vhosts
                            {connection? : The name of the queue connection to work}
                            {--name=default : The name of the consumer}
                            {--vhosts= : The list of the vhosts to work}
                            {--vhosts-mask= : The vhosts mask}
                            {--queues= : The list of the queues to work}
                            {--queues-mask= : The queues mask}
                            {--batch-size=100 : The number of jobs for batch}
                            {--once : Only process the next job on the queue}
                            {--stop-when-empty : Stop when the queue is empty}
                            {--delay=0 : The number of seconds to delay failed jobs (Deprecated)}
                            {--backoff=0 : The number of seconds to wait before retrying a job that encountered an uncaught exception}
                            {--max-jobs=0 : The number of jobs to process before stopping}
                            {--max-time=0 : The maximum number of seconds the worker should run}
                            {--force : Force the worker to run even in maintenance mode}
                            {--memory=128 : The memory limit in megabytes}
                            {--sleep=3 : Number of seconds to sleep when no job is available}
                            {--timeout=60 : The number of seconds a child process can run}
                            {--tries=1 : Number of times to attempt a job before logging it failed}
                            {--rest=0 : Number of seconds to rest between jobs}

                            {--max-priority=}
                            {--consumer-tag}
                            {--prefetch-size=0}
                            {--prefetch-count=1000}
                           ';

    protected $description = 'Consume messages';

    public function handle(): void
    {
        /** @var VhostsConsumer $consumer */
        $consumer = $this->worker;

        $consumer->setOutput($this->getOutput());

        $consumer->setContainer($this->laravel);
        $consumer->setName($this->option('name'));
        $consumer->setConsumerTag($this->consumerTag());
        $consumer->setMaxPriority((int) $this->option('max-priority'));
        $consumer->setPrefetchSize((int) $this->option('prefetch-size'));
        $consumer->setPrefetchCount((int) $this->option('prefetch-count'));
        $consumer->setBatchSize((int) $this->option('batch-size'));

        $filtersDto = new ConsumeVhostsFiltersDto(
            trim($this->option('vhosts', '')),
            trim($this->option('vhosts-mask', '')),
            trim($this->option('queues', '')),
            trim($this->option('queues-mask', ''))
        );
        $consumer->setFiltersDto($filtersDto);

        if ($this->downForMaintenance() && $this->option('once')) {
            $consumer->sleep($this->option('sleep'));
            return;
        }

        // We'll listen to the processed and failed events so we can write information
        // to the console as jobs are processed, which will let the developer watch
        // which jobs are coming through a queue and be informed on its progress.
        $this->listenForEvents();

        $connection = $this->argument('connection')
            ?: $this->laravel['config']['queue.default'];

        if (Terminal::hasSttyAvailable()) {
            $this->components->info(sprintf(
                'Processing vhosts: [%s]. Queues: [%s].',
                ($filtersDto->hasVhosts() ? implode(', ', $filtersDto->getVhosts()) : 'all'),
                ($filtersDto->hasQueues() ? implode(', ', $filtersDto->getQueues()) : 'all')
            ));
        }

        $this->runWorker(
            $connection,
            ''
        );
    }

    protected function consumerTag(): string
    {
        if ($consumerTag = $this->option('consumer-tag')) {
            return $consumerTag;
        }

        $consumerTag = implode('_', [
            Str::slug(config('app.name', 'laravel')),
            Str::slug($this->option('name')),
            md5(serialize($this->options()).Str::random(16).getmypid()),
        ]);

        return Str::substr($consumerTag, 0, 255);
    }
}


