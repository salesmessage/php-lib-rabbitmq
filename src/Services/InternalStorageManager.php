<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Services;

use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use VladimirYuldashev\LaravelQueueRabbitMQ\Dto\QueueApiDto;
use VladimirYuldashev\LaravelQueueRabbitMQ\Dto\VhostApiDto;

/**
 * https://github.com/phpredis/phpredis
 * https://redis.io/docs/latest/commands/sort/
 */
class InternalStorageManager
{
    private const INDEX_KEY_VHOSTS = 'rabbitmq_vhosts_index';

    private const INDEX_KEY_QUEUES = 'rabbitmq_queues_index';

    private PredisConnection $redis;

    public function __construct() {
        /** @var PredisConnection $redis */
        $redis = Redis::connection('persistent');
        $this->redis = $redis;
    }

    /**
     * @return void
     */
    public function removeVhostsIndex(): void
    {
        if ($this->redis->exists(self::INDEX_KEY_VHOSTS)) {
            $this->redis->del(self::INDEX_KEY_VHOSTS);
        }
    }

    /**
     * @return array
     */
    public function getVhosts(): array
    {
        $vhosts = $this->redis->sort(self::INDEX_KEY_VHOSTS, [
            'by' => '*->messages_ready',
            'sort' => 'desc',
        ]);

        return $vhosts;
    }

    /**
     * @param string $vhostName
     * @return array
     */
    public function getVhostQueues(string $vhostName): array
    {
        $indexKey = $this->getQueueIndexKey($vhostName);

        $queues = $this->redis->sort($indexKey, [
            'by' => '*->messages_ready',
           // 'limit' => [0, 100],
            'sort' => 'desc',
        ]);

        return $queues;
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return bool
     */
    public function indexVhost(VhostApiDto $vhostDto): bool
    {
        if ($vhostDto->getMessagesReady() > 0) {
            $this->addVhost($vhostDto);
            return true;
        }

        $this->removeVhost($vhostDto);
        return true;
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return void
     */
    private function addVhost(VhostApiDto $vhostDto): void
    {
        $storageKey = $this->getVhostStorageKey($vhostDto);

        if (!$this->redis->sismember(self::INDEX_KEY_VHOSTS, $storageKey)) {
            $this->redis->sadd(self::INDEX_KEY_VHOSTS, $storageKey);
        }

        $this->redis->hmset($storageKey, $vhostDto->toInternalData());
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return void
     */
    private function removeVhost(VhostApiDto $vhostDto): void
    {
        $storageKey = $this->getVhostStorageKey($vhostDto);

        if ($this->redis->sismember(self::INDEX_KEY_VHOSTS, $storageKey)) {
            $this->redis->srem(self::INDEX_KEY_VHOSTS, $storageKey);
        }

        if ($this->redis->exists($storageKey)) {
            $this->redis->del($storageKey);
        }
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return string
     */
    private function getVhostStorageKey(VhostApiDto $vhostDto): string
    {
        return sprintf('rabbitmq_vhost|%s', $vhostDto->getName());
    }

    /**
     * @param QueueApiDto $queueDto
     * @return bool
     */
    public function indexQueue(QueueApiDto $queueDto): bool
    {
        if ($queueDto->getMessagesReady() > 0) {
            $this->addQueue($queueDto);
            return true;
        }

        $this->removeQueue($queueDto);
        return true;
    }

    /**
     * @param QueueApiDto $queueDto
     * @return void
     */
    private function addQueue(QueueApiDto $queueDto): void
    {
        $storageKey = $this->getQueueStorageKey($queueDto);
        $indexKey = $this->getQueueIndexKey($queueDto->getVhostName());

        if (!$this->redis->sismember($indexKey, $storageKey)) {
            $this->redis->sadd($indexKey, $storageKey);
        }

        $this->redis->hmset($storageKey, $queueDto->toInternalData());
    }

    /**
     * @param QueueApiDto $queueDto
     * @return void
     */
    private function removeQueue(QueueApiDto $queueDto): void
    {
        $storageKey = $this->getQueueStorageKey($queueDto);
        $indexKey = $this->getQueueIndexKey($queueDto->getVhostName());

        if ($this->redis->sismember($indexKey, $storageKey)) {
            $this->redis->srem($indexKey, $storageKey);
        }

        if ($this->redis->exists($storageKey)) {
            $this->redis->del($storageKey);
        }
    }

    /**
     * @param VhostApiDto $vhostDto
     * @return string
     */
    private function getQueueStorageKey(QueueApiDto $queueDto): string
    {
        return $this->getQueueStorageKeyPrefix($queueDto->getVhostName()) . $queueDto->getName();
    }

    /**
     * @param string $vhostName
     * @return string
     */
    private function getQueueStorageKeyPrefix(string $vhostName): string
    {
        return sprintf('rabbitmq_queue|%s|', $vhostName);
    }

    /**
     * @param string $vhostName
     * @return string
     */
    private function getQueueIndexKey(string $vhostName): string
    {
        return sprintf('%s:%s', self::INDEX_KEY_QUEUES, $vhostName);
    }


 //   public function test()
  //  {
        //        $this->redis->hset('vhost:1', 'name', 'Test 1');
//        $this->redis->hset('vhost:1', 'message_count', 100);
//        $this->redis->hset('vhost:1', 'queues', json_encode(['foo1', 'bar1', 'baz1']));
//
//        $this->redis->hmset('vhost:2', [
//            'name' => 'Test 2',
//            'message_count' => 200,
//            'queues' =>  json_encode(['foo2', 'bar2', 'baz2'])
//        ]);

        // https://github.com/phpredis/phpredis
        // get
        // $this->redis->hmget('vhost:2', ['name', 'message_count']);
        // $this->redis->hgetall('vhost:2');
        // $this->redis->hget('vhost:2', 'name');

        // keys
        // $this->redis->keys('vhost:*');




        //        $this->redis->del('indices');
//        $this->redis->sAdd('indices', 'h1');
//        $this->redis->sAdd('indices', 'h2');
//        $this->redis->sAdd('indices', 'h3');
//        $this->redis->sAdd('indices', 'h4');
//
//        $this->redis->hset('h1', 'score', 2);
//        $this->redis->hset('h2', 'score', 1);
//        $this->redis->hset('h3', 'score', 3);
//        $this->redis->hset('h4', 'score', 4);
//
//      //  $this->redis->sMembers('indices');
//
//        echo '<pre>';
//        print_r( $this->redis->sort('indices', ['by' => '*->score'])    ) ;
//        echo '</pre>';
//        exit;


        //  $this->redis->del('vhosts');

//        echo '<pre>';
//        print_r( $this->redis->sort('vhosts', ['by' => '*->messages_ready']) );
//        echo '</pre>';
//        exit;

//    }


}

