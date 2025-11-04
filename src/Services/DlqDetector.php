<?php

namespace Salesmessage\LibRabbitMQ\Services;

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class DlqDetector
{
    public static function isDlqMessage(AMQPMessage $message): bool
    {
        $headersTable = $message->get_properties()['application_headers'] ?? null;

        if (!($headersTable instanceof AMQPTable)) {
            return false;
        }

        $headers = $headersTable->getNativeData();

        return !empty($headers['x-death']) && !empty($headers['x-opt-deaths']);
    }
}
