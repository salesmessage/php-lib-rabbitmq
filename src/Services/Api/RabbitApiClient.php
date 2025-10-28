<?php

namespace Salesmessage\LibRabbitMQ\Services\Api;

use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\RequestOptions;
use Salesmessage\LibRabbitMQ\Exceptions\RabbitApiClientException;

class RabbitApiClient
{
    private HttpClient $client;

    private array $connectionConfig = [];

    public function __construct()
    {
        $this->client = new HttpClient([
            RequestOptions::TIMEOUT => 30,
            RequestOptions::CONNECT_TIMEOUT => 30,
        ]);
    }

    /**
     * @param array $connectionConfig
     * @return $this
     */
    public function setConnectionConfig(array $connectionConfig): self
    {
        $this->connectionConfig = $connectionConfig;
        return $this;
    }

    /**
     * @param string $method
     * @param string $uri
     * @param array $query
     * @param array $data
     * @param array $extraHeaders
     * @return array
     * @throws RabbitApiClientException
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function request(
        string $method,
        string $uri,
        array $query = [],
        array $data = [],
        array $extraHeaders = []
    ): array {
        if (!empty($query)) {
            $options[RequestOptions::QUERY] = $query;
        }
        if (!empty($data)) {
            $options[RequestOptions::JSON] = $data;
        }

        $headers = [
            'Content-Type' => 'application/json',
            'Accept' => 'application/json',
            'Authorization' => 'Basic ' . base64_encode($this->getUsername() . ':' . $this->getPassword()),
        ];
        if (!empty($extraHeaders)) {
            $headers = array_merge($headers, $extraHeaders);
        }
        $options[RequestOptions::HEADERS] = $headers;

        try {
            $response = $this->client->request($method, $this->getBaseUrl() . $uri, $options);
            $contents = $response->getBody()->getContents();

            return (array) ($contents ? json_decode($contents, true) : []);
        } catch (\Throwable $exception) {
            $rethrowException = $exception;
            if ($exception instanceof ClientException) {
                $rethrowException = new RabbitApiClientException($exception->getMessage());
            }

            throw $rethrowException;
        }
    }

    /**
     * @return string
     */
    private function getBaseUrl(): string
    {
        $host = $this->connectionConfig['hosts'][0]['api_host'] ?? '';
        $port = $this->connectionConfig['hosts'][0]['api_port'] ?? '';

        $scheme = $this->connectionConfig['secure'] ? 'https://' : 'http://';

        return $scheme . $host . ':' . $port;
    }

    /**
     * @return string
     */
    public function getUsername(): string
    {
        return (string) ($this->connectionConfig['hosts'][0]['user'] ?? '');

    }

    /**
     * @return string
     */
    private function getPassword(): string
    {
        return (string) ($this->connectionConfig['hosts'][0]['password'] ?? '');
    }
}
