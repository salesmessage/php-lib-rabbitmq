<?php

namespace Salesmessage\LibRabbitMQ\Exceptions;

class DeliveryLimitReachedException extends \RuntimeException
{
    public function __construct(int $deliveryCount, int $limit)
    {
        parent::__construct(sprintf('Delivery limit reached: x-delivery-count %d, limit %d.', $deliveryCount, $limit));
    }
}
