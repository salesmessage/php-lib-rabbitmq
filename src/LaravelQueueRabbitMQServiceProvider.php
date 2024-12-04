<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\Connectors\BeanstalkdConnector;
use Illuminate\Queue\Connectors\DatabaseConnector;
use Illuminate\Queue\Connectors\NullConnector;
use Illuminate\Queue\Connectors\RedisConnector;
use Illuminate\Queue\Connectors\SqsConnector;
use Illuminate\Queue\Connectors\SyncConnector;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use VladimirYuldashev\LaravelQueueRabbitMQ\Console\ConsumeCommand;
use VladimirYuldashev\LaravelQueueRabbitMQ\Console\ConsumeVhostsCommand;
use VladimirYuldashev\LaravelQueueRabbitMQ\Console\ScanVhostsCommand;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Connectors\RabbitMQConnector;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\QueueManager as RabbitMQQueueManager;
use VladimirYuldashev\LaravelQueueRabbitMQ\Services\Api\RabbitApiClient;
use VladimirYuldashev\LaravelQueueRabbitMQ\Services\InternalStorageManager;

class LaravelQueueRabbitMQServiceProvider extends ServiceProvider
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

            $this->app->singleton('rabbitmq_queue', function ($app) {
                return tap(new RabbitMQQueueManager($app), function (RabbitMQQueueManager $manager) {
                    $manager->addConnector('null', function () {
                        return new NullConnector;
                    });
                    $manager->addConnector('sync', function () {
                        return new SyncConnector;
                    });
                    $manager->addConnector('database', function () {
                        return new DatabaseConnector($this->app['db']);
                    });
                    $manager->addConnector('redis', function () {
                        return new RedisConnector($this->app['redis']);
                    });
                    $manager->addConnector('beanstalkd', function () {
                        return new BeanstalkdConnector;
                    });
                    $manager->addConnector('sqs', function () {
                        return new SqsConnector;
                    });
                    $manager->addConnector('rabbitmq', function () {
                        return new RabbitMQConnector($this->app['events']);
                    });
                });
            });

            $this->app->singleton(VhostsConsumer::class, function () {
                $isDownForMaintenance = function () {
                    return $this->app->isDownForMaintenance();
                };

                return new VhostsConsumer(
                    $this->app['rabbitmq_queue'],
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
                    $app[RabbitApiClient::class],
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

        $queue->addConnector('rabbitmq', function () {
            return new RabbitMQConnector($this->app['events']);
        });
    }
}
