<?php

return [
    'consumer_type' => env('RABBITMQ_VHOSTS_CONSUMER_TYPE', 'direct'),
    /**
     * Provided on 2 levels: transport and application.
     */
    'deduplication' => [
        'transport' => [
            'enabled' => env('RABBITMQ_DEDUP_TRANSPORT_ENABLED', true),
            'ttl' => env('RABBITMQ_DEDUP_TRANSPORT_TTL', 3600),
            'lock_ttl' => env('RABBITMQ_DEDUP_TRANSPORT_LOCK_TTL', 60),
            /**
             * Possible: ack, reject
             */
            'action_on_duplication' => env('RABBITMQ_DEDUP_TRANSPORT_ACTION', 'ack'),
            /**
             * Possible: ack, reject, requeue
             */
            'action_on_lock' => env('RABBITMQ_DEDUP_TRANSPORT_LOCK_ACTION', 'requeue'),
            /**
             * Max times a locked ("in_progress") message is requeued via the TTL delay
             * queue before it is rejected. Only used when action_on_lock = requeue.
             */
            'lock_requeue_max_attempts' => env('RABBITMQ_DEDUP_TRANSPORT_LOCK_REQUEUE_MAX_ATTEMPTS', 2),
            'connection' => [
                'driver' => env('RABBITMQ_DEDUP_TRANSPORT_DRIVER', 'redis'),
                'name' => env('RABBITMQ_DEDUP_TRANSPORT_CONNECTION_NAME', 'persistent'),
                'key_prefix' => env('RABBITMQ_DEDUP_TRANSPORT_KEY_PREFIX', 'core_mq_dedup'),
            ],
        ],
        'application' => [
            'enabled' => env('RABBITMQ_DEDUP_APP_ENABLED', true),
        ],
    ],
];

