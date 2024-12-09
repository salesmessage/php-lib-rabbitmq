<?php

namespace Salesmessage\LibRabbitMQ\Dto;

class QueueApiDto
{
    private string $name = '';

    private string $vhostName = '';

    private int $messages = 0;

    private int $messagesReady = 0;

    private int $messagesUnacknowledged = 0;

    private int $lastProcessedAt = 0;

    /**
     * @param array $data
     */
    public function __construct(array $data)
    {
        $this->name = (string) ($data['name'] ?? '');
        $this->vhostName = (string) ($data['vhost'] ?? '');

        $this->messages = (int) ($data['messages'] ?? 0);
        $this->messagesReady = (int) ($data['messages_ready'] ?? 0);
        $this->messagesUnacknowledged = (int) ($data['messages_unacknowledged'] ?? 0);
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function getVhostName(): string
    {
        return $this->vhostName;
    }

    /**
     * @return int
     */
    public function getMessages(): int
    {
        return $this->messages;
    }

    /**
     * @return int
     */
    public function getMessagesReady(): int
    {
        return $this->messagesReady;
    }

    /**
     * @return int
     */
    public function getMessagesUnacknowledged(): int
    {
        return $this->messagesUnacknowledged;
    }

    /**
     * @return int
     */
    public function getLastProcessedAt(): int
    {
        return $this->lastProcessedAt;
    }

    /**
     * @param int $lastProcessedAt
     * @return $this
     */
    public function setLastProcessedAt(int $lastProcessedAt): self
    {
        $this->lastProcessedAt = $lastProcessedAt;
        return $this;
    }

    /**
     * @return array
     */
    public function toInternalData(bool $withLastProcessedAt = false): array
    {
        $data = [
            'name' => $this->getName(),
            'vhost' => $this->getVhostName(),
            'messages' => $this->getMessages(),
            'messages_ready' => $this->getMessagesReady(),
            'messages_unacknowledged' => $this->getMessagesUnacknowledged(),
        ];
        if ($withLastProcessedAt) {
            $data['last_processed_at'] = $this->getLastProcessedAt();
        }

        return $data;
    }
}

