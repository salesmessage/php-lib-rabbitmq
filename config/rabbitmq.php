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
        'queue' => [
            /**
             * Publisher confirms (ack on producing) are opted into per job by implementing
             * shouldConfirmOnPublish(): bool. When a job returns true, its publish is routed
             * to a confirm-mode channel and blocks until the broker confirms the message; a
             * broker nack, an unroutable (returned) message, or a timeout throws, so an
             * unconfirmed publish fails the dispatch instead of being silently lost.
             *
             * This option only sets the seconds to wait for a broker confirm before giving
             * up; 0 waits indefinitely. Adds a synchronous round-trip per confirmed publish.
             *
             * @see https://www.rabbitmq.com/confirms.html#publisher-confirms
             */
            'publisher_confirm_timeout' => env('RABBITMQ_PUBLISHER_CONFIRM_TIMEOUT', 5.0),
            /**
             * Quorum queues only: initial number of queue members (nodes) that replicate the
             * queue - set as x-quorum-initial-group-size. A publisher confirm is issued once a
             * majority of these members have persisted the message, so this governs how many
             * nodes must ack a publish. Leave null to use RabbitMQ's own default (3).
             */
            'quorum_initial_group_size' => env('RABBITMQ_QUORUM_INITIAL_GROUP_SIZE'),
        ],
        'quorum' => [
            /**
             * Max allowed delivery attempts (RabbitMQ x-delivery-count) for quorum queues.
             * 0 disables the check.
             */
            'delivery_limit' => env('RABBITMQ_QUORUM_DELIVERY_LIMIT', 2),
            /**
             * Action when delivery_limit is reached.
             * Possible values: 'ack', 'reject'
             */
            'on_limit_action' => env('RABBITMQ_QUORUM_ON_LIMIT_ACTION', 'reject'),
        ],
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
            /**
             * Max times a locked ("in_progress") message is requeued via the TTL delay
             * queue before it is rejected. Only used when action_on_lock = requeue.
             */
            'lock_requeue_max_attempts' => env('RABBITMQ_DEDUP_TRANSPORT_LOCK_REQUEUE_MAX_ATTEMPTS', 1),
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
    'debug' => env('RABBITMQ_VHOST_DEBUG', false),
];
