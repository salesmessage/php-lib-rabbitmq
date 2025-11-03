<?php

namespace Salesmessage\LibRabbitMQ\Queue\Jobs;

use Illuminate\Contracts\Encryption\Encrypter;
use Illuminate\Queue\Jobs\JobName;
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob as BaseJob;

/**
 * SQS Job wrapper for RabbitMQ
 */
class RabbitMQJobBatchable extends BaseJob
{
    /**
     * Fire the job.
     *
     * @return void
     */
    public function fire()
    {
        $payload = $this->payload();

        [$class, $method] = JobName::parse($payload['job']);

        ($this->instance = $this->resolve($class))->{$method}($this, $payload['data']);
    }

    /**
     * Returns target class name
     *
     * @return mixed
     */
    public function getPayloadClass(): string
    {
        $payload = $this->payload();

        return $payload['data']['commandName'];
    }

    public function release($delay = 0): void
    {
        $consumableJob = $this->getPayloadData();
        if (!($consumableJob instanceof RabbitMQConsumable)) {
            throw new \RuntimeException('Job must be an instance of RabbitMQJobBatchable');
        }

        // Always create a new message when this Job is released
        $this->rabbitmq->laterRaw($delay, $this->message->getBody(), $this->queue, $this->attempts(), $consumableJob->getQueueType());

        // Releasing a Job means the message was failed to process.
        // Because this Job message is always recreated and pushed as new message, this Job message is correctly handled.
        // We must tell rabbitMQ this job message can be removed by acknowledging the message.
        $this->rabbitmq->ack($this);
    }

    /**
     * @return object
     * @throws \RuntimeException
     */
    public function getPayloadData(): object
    {
        $payload = $this->payload();

        $data = $payload['data'];

        if (str_starts_with($data['command'], 'O:')) {
            return unserialize($data['command']);
        }

        if ($this->container->bound(Encrypter::class)) {
            return unserialize($this->container[Encrypter::class]->decrypt($data['command']));
        }

        throw new \RuntimeException('Unable to extract job data.');
    }
}
