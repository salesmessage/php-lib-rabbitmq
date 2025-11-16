<?php

namespace Salesmessage\LibRabbitMQ;

use Illuminate\Cache\RedisStore;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use Psr\Log\LoggerInterface;
use Salesmessage\LibRabbitMQ\Console\ConsumeCommand;
use Salesmessage\LibRabbitMQ\Console\ConsumeVhostsCommand;
use Salesmessage\LibRabbitMQ\Console\ScanVhostsCommand;
use Salesmessage\LibRabbitMQ\Queue\Connectors\RabbitMQVhostsConnector;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationService;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\DeduplicationStore;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\NullDeduplicationStore;
use Salesmessage\LibRabbitMQ\Services\Deduplication\TransportLevel\RedisDeduplicationStore;
use Salesmessage\LibRabbitMQ\Services\GroupsService;
use Salesmessage\LibRabbitMQ\Services\InternalStorageManager;
use Salesmessage\LibRabbitMQ\Services\QueueService;
use Salesmessage\LibRabbitMQ\Services\VhostsService;
use Salesmessage\LibRabbitMQ\Services\DeliveryLimitService;
use Salesmessage\LibRabbitMQ\VhostsConsumers\DirectConsumer as VhostsDirectConsumer;
use Salesmessage\LibRabbitMQ\VhostsConsumers\QueueConsumer as VhostsQueueConsumer;

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
            $this->bindDeduplicationService();

            $this->app->bind(LockProvider::class, RedisStore::class);

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

            $this->app->singleton(VhostsDirectConsumer::class, function () {
                $isDownForMaintenance = function () {
                    return $this->app->isDownForMaintenance();
                };

                return new VhostsDirectConsumer(
                    $this->app[InternalStorageManager::class],
                    $this->app[LoggerInterface::class],
                    $this->app['queue'],
                    $this->app['events'],
                    $this->app[ExceptionHandler::class],
                    $isDownForMaintenance,
                    $this->app->get(DeduplicationService::class),
                    $this->app->get(DeliveryLimitService::class),
                    null,
                );
            });

            $this->app->singleton(VhostsQueueConsumer::class, function () {
                $isDownForMaintenance = function () {
                    return $this->app->isDownForMaintenance();
                };

                return new VhostsQueueConsumer(
                    $this->app[InternalStorageManager::class],
                    $this->app[LoggerInterface::class],
                    $this->app['queue'],
                    $this->app['events'],
                    $this->app[ExceptionHandler::class],
                    $isDownForMaintenance,
                    $this->app->get(DeduplicationService::class),
                    $this->app->get(DeliveryLimitService::class),
                    null,
                );
            });

            $this->app->singleton(ConsumeVhostsCommand::class, static function ($app) {
                $consumerClass = ('direct' === config('queue.connections.rabbitmq_vhosts.consumer_type'))
                    ? VhostsDirectConsumer::class
                    : VhostsQueueConsumer::class;

                return new ConsumeVhostsCommand(
                    $app[GroupsService::class],
                    $app[$consumerClass],
                    $app['cache.store']
                );
            });

            $this->app->singleton(ScanVhostsCommand::class, static function ($app) {
                return new ScanVhostsCommand(
                    $app[GroupsService::class],
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

    /**
     * Config params:
     * @phpstan-import-type DeduplicationConfig from DeduplicationService
     *
     * @return void
     */
    private function bindDeduplicationService(): void
    {
        $this->app->bind(DeduplicationStore::class, static function () {
            /** @var DeduplicationConfig $config */
            $config = (array) config('queue.connections.rabbitmq_vhosts.deduplication.transport', []);
            $enabled = (bool) ($config['enabled'] ?? false);
            if (!$enabled) {
                return new NullDeduplicationStore();
            }

            $connectionDriver = $config['connection']['driver'] ?? null;
            if ($connectionDriver !== 'redis') {
                throw new \InvalidArgumentException('For now only Redis connection is supported for deduplication');
            }
            $connectionName = $config['connection']['name'] ?? null;

            $prefix = trim($config['connection']['key_prefix'] ?? '');
            if (empty($prefix)) {
                throw new \InvalidArgumentException('Key prefix is required');
            }

            return new RedisDeduplicationStore($connectionName, $prefix);
        });
    }
}
