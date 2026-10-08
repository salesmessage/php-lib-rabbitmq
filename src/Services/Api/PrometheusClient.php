<?php

namespace Salesmessage\LibRabbitMQ\Services\Api;

use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Promise\Utils;
use GuzzleHttp\RequestOptions;
use Psr\Http\Message\StreamInterface;
use Salesmessage\LibRabbitMQ\Exceptions\PrometheusMetricsException;
use Throwable;

class PrometheusClient
{
    private ClientInterface $client;

    public function __construct(ClientInterface $client = null)
    {
        $this->client = $client ?? new HttpClient;
    }

    /**
     * Fetch one metric family from the rabbitmq_prometheus detailed endpoint of every
     * host in parallel. Fails as a whole when any host fails, so callers never get a
     * partial view of the cluster.
     *
     * @param  array<string>  $hosts
     * @return array<string, StreamInterface> decoded response body per host
     *
     * @throws PrometheusMetricsException
     */
    public function fetchDetailedFamily(array $hosts, int $port, string $family, float $timeout): array
    {
        $promises = [];
        foreach ($hosts as $host) {
            $promises[$host] = $this->client->requestAsync(
                'GET',
                sprintf('http://%s:%d/metrics/detailed', $host, $port),
                [
                    RequestOptions::QUERY => ['family' => $family],
                    RequestOptions::HEADERS => ['Accept-Encoding' => 'gzip'],
                    RequestOptions::DECODE_CONTENT => true,
                    RequestOptions::TIMEOUT => $timeout,
                    RequestOptions::CONNECT_TIMEOUT => $timeout,
                ]
            );
        }

        $bodies = [];
        $failures = [];
        foreach (Utils::settle($promises)->wait() as $host => $result) {
            if ($result['state'] === PromiseInterface::FULFILLED) {
                $bodies[$host] = $result['value']->getBody();

                continue;
            }

            $reason = $result['reason'];
            $failures[] = sprintf('%s: %s', $host, $reason instanceof Throwable ? $reason->getMessage() : (string) $reason);
        }

        if (! empty($failures)) {
            foreach ($bodies as $body) {
                $body->close();
            }

            throw new PrometheusMetricsException('Failed to fetch Prometheus metrics. '.implode('; ', $failures));
        }

        return $bodies;
    }
}
