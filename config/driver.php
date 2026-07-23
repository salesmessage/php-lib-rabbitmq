<?php

return [
    'consumer_type' => env('RABBITMQ_VHOSTS_CONSUMER_TYPE', 'direct'),

    /**
     * Vhost round-robin scheduler options.
     *
     * The scheduler strategy (last_processed | processing_time) is set per group
     * in rabbit-groups.yml via `scheduler_strategy`, not here. These are only the tuning
     * options for the processing_time scheduler.
     */
    'scheduler' => [
        'strategies' => [
            'processing_time' => [
                'window' => env('RABBITMQ_VHOSTS_SCHEDULER_WINDOW', 300), // seconds
                'bucket' => env('RABBITMQ_VHOSTS_SCHEDULER_BUCKET', 30),  // seconds
                /**
                 * Provisional cost (seconds) charged to a vhost the moment a worker
                 * picks it, reconciled exactly once real processing time is recorded
                 * and refunded when the worker moves on without doing any work.
                 * Stops many simultaneous workers from piling onto the same vhost.
                 * Set to 0 to disable.
                 */
                'reservation_estimate' => env('RABBITMQ_VHOSTS_SCHEDULER_RESERVATION_ESTIMATE', 3),
                /**
                 * How often (seconds) an async consumer flushes the processing time
                 * accrued by an in-flight job, so a long-running job is reflected in
                 * the fairness ordering before it finishes instead of only at the end.
                 * Async (Swoole) mode only.
                 */
                'accrual_interval' => env('RABBITMQ_VHOSTS_SCHEDULER_ACCRUAL_INTERVAL', 7),
            ],
        ],
    ],
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
            'lock_requeue_max_attempts' => env('RABBITMQ_DEDUP_TRANSPORT_LOCK_REQUEUE_MAX_ATTEMPTS', 1),
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

