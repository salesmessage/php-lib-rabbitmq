<?php

namespace Salesmessage\LibRabbitMQ\Queue;

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Queue\QueueManager as BaseQueueManager;

class QueueManager extends BaseQueueManager
{
    public function __construct(Application $app)
    {
        parent::__construct($app);
    }

    /**
     * @param string $vhost
     * @param $name
     * @return \Illuminate\Contracts\Queue\Queue|mixed
     */
    public function rabbitConnectionByVhost(string $vhost = '/', $name = null)
    {
        $configName = $name ?: $this->getDefaultDriver();

        $config = $this->getConfig($configName);
        if (is_null($config)) {
            throw new InvalidArgumentException("The [{$configName}] queue connection has not been configured.");
        }

        $config['hosts'][0]['vhost'] = $vhost;

        $connectionName = $configName . '_' . ('/' === $vhost ? 'default' : $vhost);

        if (!isset($this->connections[$connectionName])) {
            $this->connections[$connectionName] = $this->getConnector($config['driver'])
                ->connect($config)
                ->setConnectionName($connectionName);

            $this->connections[$connectionName]->setContainer($this->app);
        }

        return $this->connections[$connectionName];
    }

    /**
     * @param string $connectionName
     * @return void
     */
    public function rabbitConnectionRemove(string $connectionName): void
    {
        if ('' === $connectionName) {
            return;
        }
        if (!isset($this->connections[$connectionName])) {
            return;
        }

        unset($this->connections[$connectionName]);
    }
}

