<?php

namespace Salesmessage\LibRabbitMQ\Tests\Mocks;

use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPSSLConnection;

class TestSSLConnection extends AMQPSSLConnection
{
    public function getConfig(): ?AMQPConnectionConfig
    {
        return $this->config;
    }
}
