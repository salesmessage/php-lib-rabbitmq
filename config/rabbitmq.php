<?php

/**
 * This is an example of queue connection configuration.
 * It will be merged into config/queue.php.
 * You need to set proper values in `.env`.
 */
return [

    'driver' => 'rabbitmq_vhosts',
    'queue' => env('RABBITMQ_QUEUE', 'default'),
    'connection' => 'default',
    'immediate_indexation' => env('RABBITMQ_IMMEDIATE_INDEXATION', false),

    'hosts' => [
        [
            'host' => env('RABBITMQ_HOST', '127.0.0.1'),
            'port' => env('RABBITMQ_PORT', 5672),
            'user' => env('RABBITMQ_USER', 'guest'),
            'password' => env('RABBITMQ_PASSWORD', 'guest'),
            'vhost' => env('RABBITMQ_VHOST', '/'),
        ],
    ],

    'options' => [
    ],

    /**
     * Provided on 2 levels: transport and application.
     */
    'deduplication' => [
        'transport' => [
            'enabled' => env('RABBITMQ_DEDUP_TRANSPORT_ENABLED', false),
            'ttl' => env('RABBITMQ_DEDUP_TRANSPORT_TTL', 7200),
            'lock_ttl' => env('RABBITMQ_DEDUP_TRANSPORT_LOCK_TTL', 60),
            /**
             * Possible: ack, reject
             */
            'action_on_duplication' => env('RABBITMQ_DEDUP_TRANSPORT_ACTION', 'ack'),
            /**
             * Possible: ack, reject, requeue
             */
            'action_on_lock' => env('RABBITMQ_DEDUP_TRANSPORT_LOCK_ACTION', 'requeue'),
            'connection' => [
                'driver' => env('RABBITMQ_DEDUP_TRANSPORT_DRIVER', 'redis'),
                'name' => env('RABBITMQ_DEDUP_TRANSPORT_CONNECTION_NAME', 'persistent'),
                'key_prefix' => env('RABBITMQ_DEDUP_TRANSPORT_KEY_PREFIX', 'mq_dedup'),
            ],
        ],
        'application' => [
            'enabled' => env('RABBITMQ_DEDUP_APP_ENABLED', true),
        ],
    ],

    /*
     * Set to "horizon" if you wish to use Laravel Horizon.
     */
    'worker' => env('RABBITMQ_WORKER', 'default'),

    /*
     * Vhost prefix for organization-specific vhosts.
     */
    'vhost_prefix' => env('RABBITMQ_VHOST_PREFIX', 'organization_'),

];
