<?php

namespace Salesmessage\LibRabbitMQ;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\Connectors\BeanstalkdConnector;
use Illuminate\Queue\Connectors\DatabaseConnector;
use Illuminate\Queue\Connectors\NullConnector;
use Illuminate\Queue\Connectors\RedisConnector;
use Illuminate\Queue\Connectors\SqsConnector;
use Illuminate\Queue\Connectors\SyncConnector;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use Salesmessage\LibRabbitMQ\Console\ConsumeCommand;
use Salesmessage\LibRabbitMQ\Console\ConsumeVhostsCommand;
use Salesmessage\LibRabbitMQ\Console\ScanVhostsCommand;
use Salesmessage\LibRabbitMQ\Queue\Connectors\RabbitMQVhostsConnector;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\QueueService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;

class LaravelLibRabbitMQServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__.'/../config/rabbitmq.php',
            'queue.connections.rabbitmq'
        );

        if ($this->app->runningInConsole()) {
            $this->app->singleton('rabbitmq.consumer', function () {
                $isDownForMaintenance = function () {
                    return $this->app->isDownForMaintenance();
                };

                return new Consumer(
                    $this->app['queue'],
                    $this->app['events'],
                    $this->app[ExceptionHandler::class],
                    $isDownForMaintenance
                );
            });

            $this->app->singleton(ConsumeCommand::class, static function ($app) {
                return new ConsumeCommand(
                    $app['rabbitmq.consumer'],
                    $app['cache.store']
                );
            });

            $this->app->singleton(VhostsConsumer::class, function () {
                $isDownForMaintenance = function () {
                    return $this->app->isDownForMaintenance();
                };

                return new VhostsConsumer(
                    $this->app[InternalStorageManager::class],
                    $this->app['queue'],
                    $this->app['events'],
                    $this->app[ExceptionHandler::class],
                    $isDownForMaintenance,
                    null
                );
            });

            $this->app->singleton(ConsumeVhostsCommand::class, static function ($app) {
                return new ConsumeVhostsCommand(
                    $app[VhostsConsumer::class],
                    $app['cache.store']
                );
            });

            $this->app->singleton(ScanVhostsCommand::class, static function ($app) {
                return new ScanVhostsCommand(
                    $app[VhostsService::class],
                    $app[QueueService::class],
                    $app[InternalStorageManager::class]
                );
            });

            $this->commands([
                Console\ConsumeCommand::class,

                Console\ConsumeVhostsCommand::class,
                Console\ScanVhostsCommand::class,
            ]);
        }

        $this->commands([
            Console\ExchangeDeclareCommand::class,
            Console\ExchangeDeleteCommand::class,
            Console\QueueBindCommand::class,
            Console\QueueDeclareCommand::class,
            Console\QueueDeleteCommand::class,
            Console\QueuePurgeCommand::class,
        ]);
    }

    /**
     * Register the application's event listeners.
     */
    public function boot(): void
    {
        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        $queue->addConnector('rabbitmq_vhosts', function () {
            return new RabbitMQVhostsConnector($this->app['events']);
        });
    }
}
