RabbitMQ Queue driver for Laravel
======================
[![Latest Stable Version](https://poser.pugx.org/vladimir-yuldashev/laravel-queue-rabbitmq/v/stable?format=flat-square)](https://packagist.org/packages/vladimir-yuldashev/laravel-queue-rabbitmq)
[![Build Status](https://github.com/vyuldashev/laravel-queue-rabbitmq/actions/workflows/tests.yml/badge.svg?branch=master)](https://github.com/vyuldashev/laravel-queue-rabbitmq/actions/workflows/tests.yml)
[![Total Downloads](https://poser.pugx.org/vladimir-yuldashev/laravel-queue-rabbitmq/downloads?format=flat-square)](https://packagist.org/packages/vladimir-yuldashev/laravel-queue-rabbitmq)
[![License](https://poser.pugx.org/vladimir-yuldashev/laravel-queue-rabbitmq/license?format=flat-square)](https://packagist.org/packages/vladimir-yuldashev/laravel-queue-rabbitmq)

## Support Policy

Only the latest version will get new features. Bug fixes will be provided using the following scheme:

| Package Version | Laravel Version | Bug Fixes Until |                                                                                             |
|-----------------|-----------------|-----------------|---------------------------------------------------------------------------------------------|
| 1               | 67              | July 17th, 2026 | [Documentation](https://github.com/vyuldashev/laravel-queue-rabbitmq/blob/master/README.md) |

## Installation

You can install this package via composer using this command:

```
composer require salesmessage/php-lib-rabbitmq:^1.68 --ignore-platform-reqs
```

The package will automatically register itself.

### Groups configuration

Add groups configuration to file `rabbit-groups.yml`:

> This config file is required.

#### Example `rabbit-groups.yml` file

```php
groups:
  test-group-1:
    vhosts:
      - organization_10
      - organization_11
      - organization_12
    vhosts_mask: organization
    queues:
      - test-queue-1
      - test-queue-11
    queues_mask: test
    batch_size: 100
    prefetch_count: 100
    # vhost scheduler for this group: last_processed (default) | processing_time
    scheduler_strategy: last_processed
  test-group-2:
    vhosts:
      - organization_20
      - organization_21
      - organization_22
    vhosts_mask: organization
    queues:
      - test-queue-2
      - test-queue-22
    queues_mask: test
    batch_size: 100
    prefetch_count: 100
  test-group-3:
    vhosts:
      - organization_30
      - organization_31
      - organization_32
    vhosts_mask: organization
    queues:
      - test-queue-3
      - test-queue-33
    queues_mask: test
    batch_size: 100
    prefetch_count: 100
```

#### Local `rabbit-groups.yml` file

```php
groups:
  hubspot-workflow:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-hubspot-workflow-process
      - local-vshcherbyna-hubspot-throttled
    batch_size: 100
    prefetch_count: 100
  hubspot-workflow-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-hubspot-workflow-process-quorum
      - local-vshcherbyna-hubspot-throttled-quorum
    batch_size: 100
    prefetch_count: 100
  calls-workflow:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-record-detect-voicemail
    batch_size: 100
    prefetch_count: 100
  calls-workflow-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-record-detect-voicemail-quorum
    batch_size: 100
    prefetch_count: 100
  contacts-phone-checker:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-contacts-phone-checker-lookup
    batch_size: 100
    prefetch_count: 100
  contacts-phone-checker-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-contacts-phone-checker-lookup-quorum
    batch_size: 100
    prefetch_count: 100
  calls-record-transcribe:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-record-transcribe-ai-summary
      - local-vshcherbyna-calls-record-transcribe
      - local-vshcherbyna-calls-record-transcribe-status-check
    batch_size: 100
    prefetch_count: 100
  calls-record-transcribe-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-record-transcribe-ai-summary-quorum
      - local-vshcherbyna-calls-record-transcribe-quorum
      - local-vshcherbyna-calls-record-transcribe-status-check-quorum
    batch_size: 100
    prefetch_count: 100
  calls-record-process:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-record-twilio-process
      - local-vshcherbyna-calls-record-process
      - local-vshcherbyna-calls-record-mark-as-ready
      - local-vshcherbyna-calls-record-twilio-remove
    batch_size: 100
    prefetch_count: 100
  calls-record-process-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-record-twilio-process-quorum
      - local-vshcherbyna-calls-record-process-quorum
      - local-vshcherbyna-calls-record-mark-as-ready-quorum
      - local-vshcherbyna-calls-record-twilio-remove-quorum
    batch_size: 100
    prefetch_count: 100
  calls-settings:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-settings
    batch_size: 100
    prefetch_count: 100
  calls-settings-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-settings-quorum
    batch_size: 100
    prefetch_count: 100
  core-hubspot-channel:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-core-hubspot-publish-message-to-channel
      - local-vshcherbyna-core-hubspot-search-channel-connection
    batch_size: 100
    prefetch_count: 100
  core-hubspot-channel-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-core-hubspot-publish-message-to-channel-quorum
      - local-vshcherbyna-core-hubspot-search-channel-connection-quorum
    batch_size: 100
    prefetch_count: 100
  message-media:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-message-media-optimize
    batch_size: 100
    prefetch_count: 100
  message-media-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-message-media-optimize-quorum
    batch_size: 100
    prefetch_count: 100
  sinch-send-sms:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-sinch-send-sms
    batch_size: 100
    prefetch_count: 100
  sinch-send-sms-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-sinch-send-sms-quorum
    batch_size: 100
    prefetch_count: 100
  message-processing:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-message-processing
    batch_size: 100
    prefetch_count: 100
  message-processing-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-message-processing-quorum
    batch_size: 100
    prefetch_count: 100
  calls-queue-clear:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-queue-clear
    batch_size: 100
    prefetch_count: 100
  calls-queue-clear-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-queue-clear-quorum
    batch_size: 100
    prefetch_count: 100
  calls-queue-end-after-available-time:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-queue-end-after-available-time
    batch_size: 100
    prefetch_count: 100
  calls-queue-end-after-available-time-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-calls-queue-end-after-available-time-quorum
    batch_size: 100
    prefetch_count: 100
  group-conversations-update:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-core-default-group-conversations-update
    batch_size: 100
    prefetch_count: 100
  group-conversations-update-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-core-default-group-conversations-update-quorum
    batch_size: 100
    prefetch_count: 100
  caller-id-verification:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-caller-id-verification
    batch_size: 100
    prefetch_count: 100
  caller-id-verification-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-caller-id-verification-quorum
    batch_size: 100
    prefetch_count: 100
  broadcasts:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-broadcasts
    batch_size: 100
    prefetch_count: 100
  broadcasts-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-broadcasts-quorum
    batch_size: 100
    prefetch_count: 100
  broadcasts-create-messages:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-broadcasts-create-messages
    batch_size: 100
    prefetch_count: 100
  broadcasts-create-messages-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-broadcasts-create-messages-quorum
    batch_size: 100
    prefetch_count: 100
  broadcasts-update-messages:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-broadcasts-update-messages
    batch_size: 100
    prefetch_count: 100
  broadcasts-update-messages-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-broadcasts-update-messages-quorum
    batch_size: 100
    prefetch_count: 100
  test-jobs-group:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-test-single
      - local-vshcherbyna-test-batch
    batch_size: 100
    prefetch_count: 100
  test-jobs-group-quorum:
    vhosts_mask: organization
    queues:
      - local-vshcherbyna-test-single-quorum
      - local-vshcherbyna-test-batch-quorum
    batch_size: 100
    prefetch_count: 100

```

### Connection Configuration

Add connection to `config/queue.php`:

> This is the minimal config for the rabbitMQ connection/driver to work.

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
       'driver' => 'rabbitmq_vhosts',
       'management_version' => env('RABBITMQ_VHOSTS_MANAGEMENT_VERSION', '3.12.8'),
       'hosts' => [
           [
               'host' => env('RABBITMQ_HOST', '127.0.0.1'),
               'port' => env('RABBITMQ_PORT', 5672),
               'user' => env('RABBITMQ_USER', 'guest'),
               'password' => env('RABBITMQ_PASSWORD', 'guest'),
               'vhost' => env('RABBITMQ_VHOST', '/'),
           ],
           // ...
       ],

       // ...
    ],

    // ...    
],
```

### Driver Configuration

Add connection to `config/queue.php`:

```php
'drivers' => [
    // ...

   'rabbitmq_vhosts' => [
        'consumer_type' => env('RABBITMQ_VHOSTS_CONSUMER_TYPE', 'direct'),
        /**
         * Vhost scheduler tuning options. The scheduler TYPE is set per group in
         * rabbit-groups.yml via scheduler_strategy, not here. See "Vhost Scheduler" below.
         */
        'scheduler' => [
            'strategies' => [
                'processing_time' => [
                    'window' => env('RABBITMQ_VHOSTS_SCHEDULER_WINDOW', 300), // seconds
                    'bucket' => env('RABBITMQ_VHOSTS_SCHEDULER_BUCKET', 30),  // seconds
                    'reservation_estimate' => env('RABBITMQ_VHOSTS_SCHEDULER_RESERVATION_ESTIMATE', 3), // seconds
                    'accrual_interval' => env('RABBITMQ_VHOSTS_SCHEDULER_ACCRUAL_INTERVAL', 7), // seconds
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
    ]

    // ...    
],
```


### Vhost Scheduler

When a group is consumed across many vhosts, the scheduler decides which vhost a
worker picks next. The mode is set **per group** in `rabbit-groups.yml` via
`scheduler_strategy` (so different groups can run different modes), while the tuning
options live in the driver config. Everything is tracked per `(group, vhost)` so a
vhost shared by several groups is scheduled independently in each.

```yaml
# rabbit-groups.yml
groups:
  my-group:
    vhosts_mask: organization
    queues:
      - my-queue
    scheduler_strategy: processing_time   # omit for the default (last_processed)
```

- `last_processed` (default) - recency-based round-robin. The next vhost is
  the one whose last processing happened longest ago. Distributes worker turns by
  count, so a vhost with a huge workload is only pushed back one slot per turn and
  can dominate the workers.

- `processing_time` - time-based fair round-robin. The next vhost is the one that
  consumed the least *processing time* for the group within a sliding window. This
  distributes worker capacity by time rather than by message count, preventing one
  large workload from monopolizing the workers.

> `lib-rabbitmq:scan-vhosts` needs no scheduler setting: it maintains the sliding-window
> decay for any vhost that has recorded processing time, and is a cheap no-op for the
> rest. So the scanner can never fall out of sync with a group's `scheduler_strategy`.

`processing_time` tuning options (config `queue.drivers.rabbitmq_vhosts.scheduler.strategies.processing_time`):

| Option | Env | Default | Meaning |
|--------|-----|---------|---------|
| `window` | `RABBITMQ_VHOSTS_SCHEDULER_WINDOW` | `300` | How far back (seconds) fairness looks. |
| `bucket` | `RABBITMQ_VHOSTS_SCHEDULER_BUCKET` | `30` | Resolution (seconds) of the window; keep `bucket <= window / 5`. |
| `reservation_estimate` | `RABBITMQ_VHOSTS_SCHEDULER_RESERVATION_ESTIMATE` | `3` | Provisional cost (seconds) charged on pick to spread simultaneous workers; `0` disables. |
| `accrual_interval` | `RABBITMQ_VHOSTS_SCHEDULER_ACCRUAL_INTERVAL` | `7` | How often (seconds) an async worker flushes an in-flight job's elapsed time into the ordering. Async (Swoole) mode only. |

Notes for `processing_time`:

- Processing time is accumulated as integer milliseconds into per-bucket counters
  in Redis and summed over the window; the resulting cost is used as the ordering
  key. Integer math keeps provisional reconciliation exact. Idle vhosts decay
  towards zero as their buckets fall out of the window.
- The `lib-rabbitmq:scan-vhosts` command refreshes this decay for indexed vhosts
  (unconditionally, no flag needed), so it should be running for idle vhosts to
  become eligible again promptly.
- Fairness is applied at the vhost level; queue ordering within a vhost stays
  recency-based.
- Anti-stampede (when many workers start at once): vhosts with equal cost are
  shuffled so workers do not all begin on the same vhost, and picking a vhost
  charges it a provisional `reservation_estimate` cost immediately. The
  provisional is reconciled exactly once real time is recorded, refunded when
  the worker leaves without doing any work (empty queue), and self-heals via
  bucket expiry if a worker dies mid-batch. If a single job can run much longer
  than the estimate and workers still pile onto its vhost, raise
  `reservation_estimate`.

#### How processing time is tracked

Time is accumulated into fixed **time buckets** (integer milliseconds). A vhost's
cost is the sum of the buckets still inside the window; as `now` advances the oldest
buckets fall out - that is the "slide". Everything is keyed by `(group, vhost)`.

```
group=billing  vhost=organization_5   bucket=30s   window=300s (10 buckets)

Redis hash  rabbitmq_proc_buckets|billing|organization_5
  bucketId :  B-10   B-9   B-8   B-7   B-6   B-5   B-4   B-3   B-2   B-1    B
  ms       : [ 900 ][  0 ][ 300][2000][  0 ][ 120][ 800][  0 ][ 450][1500][ 200]
              └──┬─┘ └───────────────────────────┬───────────────────────────┘
           dropped next slide          window = last 10 buckets           ▲
           (bucketId < now-window                                    current bucket
            → HDEL, not counted)                                   (writes land here)

  window_cost:billing  =  0+300+2000+0+120+800+0+450+1500+200  =  5370 ms
                          └── materialized onto rabbitmq_vhost|organization_5,
                              the field that SORT ... BY *->window_cost:billing reads ──┘
```

Write path - `record(X ms)` after a job/batch:

```
bucketId = floor(now / bucket)
HINCRBY rabbitmq_proc_buckets|billing|organization_5  <bucketId>  X   # add to current bucket
EXPIRE  rabbitmq_proc_buckets|billing|organization_5  window+bucket   # self-expire when idle
# recompute window_cost = sum(buckets with bucketId >= floor((now-window)/bucket)),
# HDEL the older ones, clamp at 0, HSET onto the vhost hash
```

Reserve / record with the provisional estimate (spreads simultaneous workers):

```
pick organization_5   → reserve(): HINCRBY current bucket  +5000   (estimate; vhost now looks busy)
job took 1200 ms      → record():  HINCRBY current bucket  (1200 - 5000)
                                   net left in the bucket = exactly 1200 ms
```

Switching modes only requires changing a group's `scheduler_strategy` in
`rabbit-groups.yml` and restarting that group's workers; the scanner needs no change.
Switching back is safe - the extra Redis keys created by `processing_time` are
TTL-bounded and expire on their own.

### Optional Queue Config

Optionally add queue options to the config of a connection.
Every queue created for this connection, gets the properties.

When you want to prioritize messages when they were delayed, then this is possible by adding extra options.

- When max-priority is omitted, the max priority is set with 2 when used.

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'prioritize_delayed' =>  false,
                'queue_max_priority' => 10,
            ],
        ],
    ],

    // ...    
],
```

When you want to publish messages against an exchange with routing-keys, then this is possible by adding extra options.

- When the exchange is omitted, RabbitMQ will use the `amq.direct` exchange for the routing-key
- When routing-key is omitted the routing-key by default is the `queue` name.
- When using `%s` in the routing-key the queue_name will be substituted.

> Note: when using an exchange with routing-key, you probably create your queues with bindings yourself.

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'exchange' => 'application-x',
                'exchange_type' => 'topic',
                'exchange_routing_key' => '',
            ],
        ],
    ],

    // ...    
],
```

In Laravel failed jobs are stored into the database. But maybe you want to instruct some other process to also do
something with the message.
When you want to instruct RabbitMQ to reroute failed messages to a exchange or a specific queue, then this is possible
by adding extra options.

- When the exchange is omitted, RabbitMQ will use the `amq.direct` exchange for the routing-key
- When routing-key is omitted, the routing-key by default the `queue` name is substituted with `'.failed'`.
- When using `%s` in the routing-key the queue_name will be substituted.

> Note: When using failed_job exchange with routing-key, you probably need to create your exchange/queue with bindings
> yourself.

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'reroute_failed' => true,
                'failed_exchange' => 'failed-exchange',
                'failed_routing_key' => 'application-x.%s',
            ],
        ],
    ],

    // ...    
],
```

By default a publish is fire-and-forget: `dispatch()` returns as soon as the message is written to
the socket, without any guarantee that RabbitMQ accepted it. When you want the broker to acknowledge
a produced message ("ack on producing"), opt the job into
[publisher confirms](https://www.rabbitmq.com/confirms.html#publisher-confirms) by implementing a
`shouldConfirmOnPublish(): bool` method on it.

- Confirms are decided **per job**, not per connection: when a job returns `true`, its publish is
  routed to a dedicated channel kept in confirm mode and blocks until the broker confirms the
  message. Jobs that return `false` (or do not implement the method) keep the fire-and-forget path
  on a separate, non-confirm channel.
- A broker `nack` (the broker could not take responsibility for the message), an unroutable
  (returned) message, or a wait timeout throws an `AMQPRuntimeException` / `AMQPTimeoutException`, so
  an unconfirmed publish fails the dispatch instead of being silently lost.
- The method is detected via `method_exists`, so existing `RabbitMQConsumable` jobs do not need to
  change - they simply keep publishing without confirms.
- It is honored across all publish paths: `push`, `later`, `bulk`/batch, and job release/retry. The
  transport-level deduplication lock-requeue republish always uses confirms (there is no job there to
  opt in, and the message must be confirmed before the original is acked).
- For quorum queues, a confirm is issued once a majority of the queue's members have persisted the
  message. The number of members is controlled by `quorum_initial_group_size`
  (`x-quorum-initial-group-size`); leave it unset to use RabbitMQ's default (3).

> Note: publisher confirms add a synchronous broker round-trip per confirmed publish. This is the
> intended trade-off for delivery guarantees, but it is noticeable in tight loops / bulk publishing.

```php
use Salesmessage\LibRabbitMQ\Contracts\RabbitMQConsumable;

class SendCriticalThing implements RabbitMQConsumable
{
    // ...

    public function shouldConfirmOnPublish(): bool
    {
        return true;
    }
}
```

Only the confirm wait timeout is configured at the connection level (seconds; `0` waits
indefinitely):

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'publisher_confirm_timeout' => 5.0,
            ],
        ],
    ],

    // ...    
],
```

### Horizon support

Starting with 8.0, this package supports [Laravel Horizon](https://laravel.com/docs/horizon) out of the box. Firstly,
install Horizon and then set `RABBITMQ_WORKER` to `horizon`.

Horizon is depending on events dispatched by the worker.
These events inform Horizon what was done with the message/job.

This Library supports Horizon, but in the config you have to inform Laravel to use the QueueApi compatible with horizon.

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        /* Set to "horizon" if you wish to use Laravel Horizon. */
       'worker' => env('RABBITMQ_WORKER', 'default'),
    ],

    // ...    
],
```

### Use your own RabbitMQJob class

Sometimes you have to work with messages published by another application.  
Those messages probably won't respect Laravel's job payload schema.
The problem with these messages is that, Laravel workers won't be able to determine the actual job or class to execute.

You can extend the build-in `RabbitMQJob::class` and within the queue connection config, you can define your own class.
When you specify a `job` key in the config, with your own class name, every message retrieved from the broker will get
wrapped by your own class.

An example for the config:

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'job' => \Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJobBatchable::class,
            ],
        ],
    ],

    // ...    
],
```

An example of your own job class:

```php
<?php

namespace App\Queue\Jobs;

use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob as BaseJob;

class RabbitMQJob extends BaseJob
{

    /**
     * Fire the job.
     *
     * @return void
     */
    public function fire()
    {
        $payload = $this->payload();

        $class = WhatheverClassNameToExecute::class;
        $method = 'handle';

        ($this->instance = $this->resolve($class))->{$method}($this, $payload);

        $this->delete();
    }
}

```

Or maybe you want to add extra properties to the payload:

```php
<?php

namespace App\Queue\Jobs;

use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob as BaseJob;

class RabbitMQJob extends BaseJob
{
   /**
     * Get the decoded body of the job.
     *
     * @return array
     */
    public function payload()
    {
        return [
            'job'  => 'WhatheverFullyQualifiedClassNameToExecute@handle',
            'data' => json_decode($this->getRawBody(), true)
        ];
    }
}
```

If you want to handle raw message, not in JSON format or without 'job' key in JSON,
you should add stub for `getName` method:

```php
<?php

namespace App\Queue\Jobs;

use Illuminate\Support\Facades\Log;
use Salesmessage\LibRabbitMQ\Queue\Jobs\RabbitMQJob as BaseJob;

class RabbitMQJob extends BaseJob
{
    public function fire()
    {
        $anyMessage = $this->getRawBody();
        Log::info($anyMessage);

        $this->delete();
    }

    public function getName()
    {
        return '';
    }
}
```

### Use your own Connection

You can extend the built-in `PhpAmqpLib\Connection\AMQPStreamConnection::class`
or `PhpAmqpLib\Connection\AMQPSLLConnection::class` and within the connection config, you can define your own class.
When you specify a `connection` key in the config, with your own class name, every connection will use your own class.

An example for the config:

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'connection' = > \App\Queue\Connection\MyRabbitMQConnection::class,
    ],

    // ...    
],
```

### Use your own Worker class

If you want to use your own `RabbitMQQueue::class` this is possible by
extending `Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue`.
and inform laravel to use your class by setting `RABBITMQ_WORKER` to `\App\Queue\RabbitMQQueue::class`.

> Note: Worker classes **must** extend `Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue`

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        /* Set to a class if you wish to use your own. */
       'worker' => \Salesmessage\LibRabbitMQ\Queue\RabbitMQQueueBatchable::class
    ],

    // ...    
],
```

```php
<?php

namespace App\Queue;

use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;

class RabbitMQQueue extends BaseRabbitMQQueue
{
    // ...
}
```

**For Example: A reconnect implementation.**

If you want to reconnect to RabbitMQ, if the connection is dead.
You can override the publishing and the createChannel methods.

> Note: this is not best practice, it is an example.

```php
<?php

namespace App\Queue;

use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use Salesmessage\LibRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;

class RabbitMQQueue extends BaseRabbitMQQueue
{

    protected function publishBasic($msg, $exchange = '', $destination = '', $mandatory = false, $immediate = false, $ticket = null): void
    {
        try {
            parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket);
        } catch (AMQPConnectionClosedException|AMQPChannelClosedException) {
            $this->reconnect();
            parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket);
        }
    }

    protected function publishBatch($jobs, $data = '', $queue = null): void
    {
        try {
            parent::publishBatch($jobs, $data, $queue);
        } catch (AMQPConnectionClosedException|AMQPChannelClosedException) {
            $this->reconnect();
            parent::publishBatch($jobs, $data, $queue);
        }
    }

    protected function createChannel(): AMQPChannel
    {
        try {
            return parent::createChannel();
        } catch (AMQPConnectionClosedException) {
            $this->reconnect();
            return parent::createChannel();
        }
    }
}
```

### Default Queue

The connection does use a default queue with value 'default', when no queue is provided by laravel.
It is possible to change te default queue by adding an extra parameter in the connection config.

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...
            
        'queue' => env('RABBITMQ_QUEUE', 'default'),
    ],

    // ...    
],
```

### Immediate Indexation

By default, your connection will be created with a immediate indexation setting of `false`.


```php

'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'immediate_indexation' => env('RABBITMQ_IMMEDIATE_INDEXATION', false),
    ],

    // ...    
],
```

### Heartbeat

By default, your connection will be created with a heartbeat setting of `0`.
You can alter the heartbeat settings by changing the config.

```php

'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'options' => [
            // ...

            'heartbeat' => 10,
        ],
    ],

    // ...    
],
```

### SSL Secure

If you need a secure connection to rabbitMQ server(s), you will need to add these extra config options.

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'secure' = > true,
        'options' => [
            // ...

            'ssl_options' => [
                'cafile' => env('RABBITMQ_SSL_CAFILE', null),
                'local_cert' => env('RABBITMQ_SSL_LOCALCERT', null),
                'local_key' => env('RABBITMQ_SSL_LOCALKEY', null),
                'verify_peer' => env('RABBITMQ_SSL_VERIFY_PEER', true),
                'passphrase' => env('RABBITMQ_SSL_PASSPHRASE', null),
            ],
        ],
    ],

    // ...    
],
```

### Events after Database commits

To instruct Laravel workers to dispatch events after all database commits are completed.

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'after_commit' => true,
    ],

    // ...    
],
```

### Lazy Connection

By default, your connection will be created as a lazy connection.
If for some reason you don't want the connection lazy you can turn it off by setting the following config.

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'lazy' = > false,
    ],

    // ...    
],
```

### Network Protocol

By default, the network protocol used for connection is tcp.
If for some reason you want to use another network protocol, you can add the extra value in your config options.
Available protocols : `tcp`, `ssl`, `tls`

```php
'connections' => [
    // ...

    'rabbitmq_vhosts' => [
        // ...

        'network_protocol' => 'tcp',
    ],

    // ...    
],
```

### Octane support

Starting with 1.04, this package supports [Laravel Octane](https://laravel.com/docs/octane) out of the box.
Firstly, install Octane and don't forget to warm 'rabbitmq' connection in the octane config.
> See: https://github.com/vyuldashev/laravel-queue-rabbitmq/issues/460#issuecomment-1469851667

## Laravel Usage

Once you completed the configuration you can use the Laravel Queue API. If you used other queue drivers you do not
need to change anything else. If you do not know how to use the Queue API, please refer to the official Laravel
documentation: http://laravel.com/docs/queues

## Lumen Usage

For Lumen usage the service provider should be registered manually as follow in `bootstrap/app.php`:

```php
$app->register(Salesmessage\LibRabbitMQ\LaravelLibRabbitMQServiceProvider::class);
```

## Scan Vhosts API

```bash
php artisan lib-rabbitmq:scan-vhosts --type=api --max-memory=200 --with-output=false --sleep=1
```

## Scan Vhosts Interim

```bash
php artisan lib-rabbitmq:actualize-interim-vhosts --max-memory=200 --with-output=false --sleep=1
```

```bash
php artisan lib-rabbitmq:scan-vhosts --type=interim --max-memory=200 --with-output=false --sleep=1
```

## Scan Vhosts Other Connection

```bash
php artisan lib-rabbitmq:scan-vhosts --connection=rabbitmq_cluster
```

## Consuming Messages

There are two ways of consuming messages.

1. `queue:work` command which is Laravel's built-in command. This command utilizes `basic_get`. Use this if you want to consume multiple queues.

2. `lib-rabbitmq:consume-vhosts` command which is provided by this package. This command utilizes `basic_consume` and is more performant than `basic_get` by ~2x, but does not support multiple queues.

Example:
```bash
php artisan lib-rabbitmq:consume-vhosts test-group-1 rabbitmq_vhosts --name=mq-vhosts-test-name --sleep=3 --memory=300 --max-jobs=5000 --max-time=600 --timeout=0 --async-mode=1
```

## Testing

Setup RabbitMQ using `docker-compose`:

```bash
docker compose up -d
```

To run the test suite you can use the following commands:

```bash
# To run both style and unit tests.
composer test

# To run only style tests.
composer test:style

# To run only unit tests.
composer test:unit
```

Scheduler and storage tests run against an in-memory Redis fake by default.
To run the same tests against a live Redis instead:

```bash
# keys are prefixed with sm_test_rabbitmq_lib_ and purged before/after each test
USE_REAL_REDIS=1 vendor/bin/phpunit tests/Unit

# non-default host/port
USE_REAL_REDIS=1 TEST_REDIS_HOST=127.0.0.1 TEST_REDIS_PORT=6379 vendor/bin/phpunit tests/Unit
```

If you receive any errors from the style tests, you can automatically fix most,
if not all the issues with the following command:

```bash
composer fix:style
```

## Local Setup
- Configure all config items in `config/queue.php` section `connections.rabbitmq_vhosts` (see as example [rabbitmq.php](./config/rabbitmq.php))
- Create `yml` file in the project root with name `rabbit-groups.yml` and content, for example like this (you can replace `vhosts` and `queues` with `vhosts_mask` and `queues_mask`):
```yaml
groups:
  test-notes:
    vhosts:
      - organization_200005
    queues:
      - local-myname.notes.200005
    batch_size: 3
    prefetch_count: 3
```
- Make sure that vhosts exist in RabbitMQ (if not - create them)
- Run command `php artisan lib-rabbitmq:scan-vhosts` within your project where this library is installed (this command fetches data from RabbitMQ to Redis)
- Run command for consumer `php artisan lib-rabbitmq:consume-vhosts test-notes rabbitmq_vhosts --name=mq-vhost-test-local-notes --memory=300 --timeout=0 --max-jobs=1000 --max-time=600 --async-mode=1`
