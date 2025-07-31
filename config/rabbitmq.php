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

    /*
     * Set to "horizon" if you wish to use Laravel Horizon.
     */
    'worker' => env('RABBITMQ_WORKER', 'default'),

    /*
     * Vhost prefix for organization-specific vhosts.
     */
    'vhost_prefix' => env('RABBITMQ_VHOST_PREFIX', 'organization_'),

];
