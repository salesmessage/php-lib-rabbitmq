<?php

namespace Salesmessage\LibRabbitMQ\Dto;

class VhostApiDto
{
    private string $name = '';

    private int $messages = 0;

    private int $messagesReady = 0;

    private int $messagesUnacknowledged = 0;

    private int $lastProcessedAt = 0;
    
    private ?string $groupName = null;

    /**
     * Queues of the vhost that may hold messages, by name: their counts, or null for a queue that
     * exists but no node reported counts for (e.g. a quorum queue electing a leader). Null when the
     * source does not read queues, so they have to be fetched separately.
     *
     * @var array<string, QueueApiDto|null>|null
     */
    private ?array $queues = null;

    /**
     * False when some nodes were not read, so a queue missing from $queues may still hold messages.
     */
    private bool $isQueuesComplete = true;

    /**
     * @param array $data
     */
    public function __construct(array $data)
    {
        $this->name = (string) ($data['name'] ?? '');

        $this->messages = (int) ($data['messages'] ?? 0);
        $this->messagesReady = (int) ($data['messages_ready'] ?? 0);
        $this->messagesUnacknowledged = (int) ($data['messages_unacknowledged'] ?? 0);

        if (isset($data['queues']) && is_array($data['queues'])) {
            $queues = [];
            foreach ($data['queues'] as $queueName => $counts) {
                $queues[(string) $queueName] = is_array($counts)
                    ? new QueueApiDto(['name' => (string) $queueName, 'vhost' => $this->name] + $counts)
                    : null;
            }

            $this->queues = $queues;
            $this->isQueuesComplete = (bool) ($data['queues_complete'] ?? true);
        }
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
    public function getApiName(): string
    {
        return ('/' === $this->name) ? '%2F' : $this->name;
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
     * @return array<string, QueueApiDto|null>|null
     */
    public function getQueues(): ?array
    {
        return $this->queues;
    }

    /**
     * @param  array<string, QueueApiDto|null>|null  $queues
     * @return $this
     */
    public function setQueues(?array $queues): self
    {
        $this->queues = $queues;
        return $this;
    }

    /**
     * @return bool
     */
    public function isQueuesComplete(): bool
    {
        return $this->isQueuesComplete;
    }

    /**
     * @param bool $isQueuesComplete
     * @return $this
     */
    public function setQueuesComplete(bool $isQueuesComplete): self
    {
        $this->isQueuesComplete = $isQueuesComplete;
        return $this;
    }

    /**
     * @return array
     */
    public function toInternalData(): array
    {
        return [
            'name' => $this->getName(),
            'messages' => $this->getMessages(),
            'messages_ready' => $this->getMessagesReady(),
            'messages_unacknowledged' => $this->getMessagesUnacknowledged(),
        ];
    }

    /**
     * Internal data plus the queues, when they were read. The constructor reads it back.
     *
     * @return array
     */
    public function toInterimData(): array
    {
        $data = $this->toInternalData();
        if (null === $this->queues) {
            return $data;
        }

        $data['queues'] = array_map(fn (?QueueApiDto $queueDto): ?array => (null === $queueDto) ? null : [
            'messages' => $queueDto->getMessages(),
            'messages_ready' => $queueDto->getMessagesReady(),
            'messages_unacknowledged' => $queueDto->getMessagesUnacknowledged(),
        ], $this->queues);
        $data['queues_complete'] = $this->isQueuesComplete;

        return $data;
    }
}

