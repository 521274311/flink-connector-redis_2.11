package club.kingon.flink.connectors.redis.common.mapper;

/**
 * @author dragons
 * @date 2020/11/4 11:27
 */
public enum RedisCommand {
    LPUSH(RedisDataType.LIST),
    RPUSH(RedisDataType.LIST),
    SADD(RedisDataType.SET),
    SET(RedisDataType.STRING),
    PFADD(RedisDataType.HYPER_LOG_LOG),
    PUBLISH(RedisDataType.PUBSUB),
    ZADD(RedisDataType.SORTED_SET),
    HSET(RedisDataType.HASH);

    private RedisDataType redisDataType;

    private RedisCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }

    public RedisDataType getRedisDataType() {
        return this.redisDataType;
    }
}
