<?php

namespace Salesmessage\LibRabbitMQ\Services;

use Illuminate\Contracts\Config\Repository as ConfigRepository;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Psr\Log\LoggerInterface;

class DeliveryLimitService
{
    public function __construct(
        private ConfigRepository $config,
        private LoggerInterface $logger,
    ) {}

    public function isAllowed(AMQPMessage $message): bool
    {
        $config = (array) $this->config->get('queue.connections.rabbitmq_vhosts.options', []);
        $limit = (int) ($config['quorum']['delivery_limit'] ?? 0);
        if ($limit <= 0) {
            return true;
        }

        if (!$this->isFromQuorumQueue($message)) {
            return true;
        }

        $deliveryCount = $this->getMessageDeliveryCount($message);
        if ($deliveryCount < $limit) {
            return true;
        }

        $action = strtolower((string) ($config['quorum']['on_limit_action'] ?? 'reject'));
        try {
            $this->logger->warning('Salesmessage.LibRabbitMQ.Services.DeliveryLimitService.limitReached', [
                'message_id' => $message->get_properties()['message_id'] ?? null,
                'action' => $action,
            ]);

            if ($action === 'ack') {
                $message->ack();
            } else {
                $message->reject(false);
            }
        } catch (\Throwable $exception) {
            $this->logger->error('Salesmessage.LibRabbitMQ.Services.DeliveryLimitService.handle.exception', [
                'message' => $exception->getMessage(),
                'trace' => $exception->getTraceAsString(),
                'error_class' => get_class($exception),
            ]);
        }

        return false;
    }

    private function getMessageDeliveryCount(AMQPMessage $message): int
    {
        $properties = $message->get_properties();
        /** @var AMQPTable|null $headers */
        $headers = $properties['application_headers'] ?? null;
        if (!$headers instanceof AMQPTable) {
            return 0;
        }

        return (int) ($headers->getNativeData()['x-delivery-count'] ?? 0);
    }

    private function isFromQuorumQueue(AMQPMessage $message): bool
    {
        $properties = $message->get_properties();
        /** @var AMQPTable|null $headers */
        $headers = $properties['application_headers'] ?? null;
        if (!$headers instanceof AMQPTable) {
            return false;
        }

        $data = $headers->getNativeData();

        return array_key_exists('x-delivery-count', $data);
    }
}
