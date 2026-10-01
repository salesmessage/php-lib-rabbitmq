<?php

namespace Salesmessage\LibRabbitMQ\Tests\Unit\Queue;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Salesmessage\LibRabbitMQ\Queue\DelayBuckets;

class DelayBucketsTest extends TestCase
{
    public static function bucketProvider(): array
    {
        return [
            'sub-second rounds up to 1s' => [1, 1000],
            'exact bucket is kept' => [5000, 5000],
            'just over a bucket moves to the next' => [5001, 10000],
            '4s -> 5s' => [4000, 5000],
            '37s -> 45s' => [37000, 45000],
            '61s -> 90s' => [61000, 90000],
            'one hour is the last fixed bucket' => [3600000, 3600000],
            'over an hour rounds to 15 min step' => [3601000, 4500000],
            'two hours exact' => [7200000, 7200000],
        ];
    }

    #[DataProvider('bucketProvider')]
    public function test_rounds_up_to_bucket(int $ttlMs, int $expectedMs): void
    {
        $this->assertSame($expectedMs, DelayBuckets::roundUpMs($ttlMs));
    }

    public function test_never_delivers_earlier_than_requested_and_stays_bounded(): void
    {
        $buckets = [];
        for ($seconds = 1; $seconds <= 86400; $seconds++) {
            $ms = DelayBuckets::roundUpMs($seconds * 1000);
            $this->assertGreaterThanOrEqual($seconds * 1000, $ms);
            $buckets[$ms] = true;
        }

        // 21 fixed buckets + 15-minute steps up to 24h: far fewer than 86400 distinct delays.
        $this->assertLessThanOrEqual(120, count($buckets));
    }
}
