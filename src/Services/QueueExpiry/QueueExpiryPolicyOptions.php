<?php

namespace Salesmessage\LibRabbitMQ\Services\QueueExpiry;

final class QueueExpiryPolicyOptions
{
    private const DEFAULT_NAME = 'sm-queue-expiry';

    private const DEFAULT_PATTERN = '^(?!.*failed)(?!.*dlq).+$';

    private const DEFAULT_EXPIRES_MS = 259200000;

    private const DEFAULT_PRIORITY = 0;

    private const DEFAULT_APPLY_RATE_PER_SECOND = 2.0;

    private const APPLY_TO = 'queues';

    private function __construct(
        private bool $enabled,
        private string $name,
        private string $pattern,
        private int $expiresMs,
        private int $priority,
        private float $applyRatePerSecond
    ) {}

    /**
     * @param  array  $options  raw config: enabled, name, pattern, expires_ms, priority, apply_rate_per_second
     *
     * @throws \InvalidArgumentException
     */
    public static function fromConfig(array $options): self
    {
        $expiresMs = (int) self::valueOrDefault($options, 'expires_ms', self::DEFAULT_EXPIRES_MS);
        if ($expiresMs < 1) {
            throw new \InvalidArgumentException(sprintf('Queue expiry must be a positive number of milliseconds, %d given.', $expiresMs));
        }

        return new self(
            filter_var($options['enabled'] ?? false, FILTER_VALIDATE_BOOLEAN),
            trim((string) self::valueOrDefault($options, 'name', self::DEFAULT_NAME)),
            (string) self::valueOrDefault($options, 'pattern', self::DEFAULT_PATTERN),
            $expiresMs,
            (int) self::valueOrDefault($options, 'priority', self::DEFAULT_PRIORITY),
            max(0.0, (float) self::valueOrDefault($options, 'apply_rate_per_second', self::DEFAULT_APPLY_RATE_PER_SECOND))
        );
    }

    private static function valueOrDefault(array $options, string $key, int|float|string $default): int|float|string
    {
        $value = $options[$key] ?? null;

        return ($value === null || $value === '') ? $default : $value;
    }

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getExpiresMs(): int
    {
        return $this->expiresMs;
    }

    public function getApplyRatePerSecond(): float
    {
        return $this->applyRatePerSecond;
    }

    /**
     * @return array{pattern: string, definition: array{expires: int}, priority: int, apply-to: string}
     */
    public function toPolicyBody(): array
    {
        return [
            'pattern' => $this->pattern,
            'definition' => ['expires' => $this->expiresMs],
            'priority' => $this->priority,
            'apply-to' => self::APPLY_TO,
        ];
    }

    /**
     * @param  array  $policy  an item of GET /api/operator-policies
     */
    public function matches(array $policy): bool
    {
        $expected = $this->toPolicyBody();

        return ($expected['pattern'] === ($policy['pattern'] ?? null))
            && ($expected['apply-to'] === ($policy['apply-to'] ?? null))
            && ($expected['priority'] === (int) ($policy['priority'] ?? 0))
            && ($expected['definition'] === (array) ($policy['definition'] ?? []));
    }
}
