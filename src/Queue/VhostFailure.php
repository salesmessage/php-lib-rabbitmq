<?php

namespace Salesmessage\LibRabbitMQ\Queue;

use Throwable;

/**
 * Whether a connection or channel failure means the vhost itself is gone, as opposed to an outage, a refused or
 * overloaded vhost that is healthy again a moment later. The publisher recreates a missing vhost; the vhost
 * consumers use the same checks to decide what to do with a vhost they cannot connect to.
 */
final class VhostFailure
{
    public static function isMissing(Throwable $exception, ?string $vhostName): bool
    {
        $vhostName = (string) $vhostName;

        // 530 = connection-level NOT_ALLOWED (vhost missing at connection.open on a direct broker),
        // 403 = the same reported when a channel is opened (e.g. AMQProxy, which opens the upstream then).
        if (in_array($exception->getCode(), [403, 530], true)
            && str_contains($exception->getMessage(), sprintf('NOT_ALLOWED - vhost %s not found', $vhostName))
        ) {
            return true;
        }

        return str_contains($exception->getMessage(), sprintf("CONNECTION_FORCED - vhost '%s' is deleted", $vhostName));
    }

    /**
     * The whole broker is out of reach (AMQProxy answers UPSTREAM_ERROR when it cannot reach it): every vhost
     * fails the same way, so the failure says nothing about this vhost.
     */
    public static function isBrokerUnreachable(Throwable $exception): bool
    {
        return str_contains($exception->getMessage(), 'UPSTREAM_ERROR');
    }
}
