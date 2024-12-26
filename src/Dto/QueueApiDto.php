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

    private ?string $groupName = null;

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
     * @param int $messages
     * @return $this
     */
    public function setMessages(int $messages): self
    {
        $this->messages = $messages;
        return $this;
    }

    /**
     * @return int
     */
    public function getMessagesReady(): int
    {
        return $this->messagesReady;
    }

    /**
     * @param int $messagesReady
     * @return $this
     */
    public function setMessagesReady(int $messagesReady): self
    {
        $this->messagesReady = $messagesReady;
        return $this;
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
     * @return string|null
     */
    public function getGroupName(): ?string
    {
        return $this->groupName;
    }

    /**
     * @param string|null $groupName
     * @return $this
     */
    public function setGroupName(?string $groupName): self
    {
        $this->groupName = $groupName;
        return $this;
    }

    /**
     * @return array
     */
    public function toInternalData(): array
    {
        return [
            'name' => $this->getName(),
            'vhost' => $this->getVhostName(),
            'messages' => $this->getMessages(),
            'messages_ready' => $this->getMessagesReady(),
            'messages_unacknowledged' => $this->getMessagesUnacknowledged(),
        ];
    }
}

