<?php

namespace Salesmessage\LibRabbitMQ\Contracts;

use PhpAmqpLib\Connection\AbstractConnection;
use Salesmessage\LibRabbitMQ\Queue\QueueConfig;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue;

interface RabbitMQQueueContract
{
    public function __construct(QueueConfig $config);

    public function setConnection(AbstractConnection $connection): RabbitMQQueue;
}
