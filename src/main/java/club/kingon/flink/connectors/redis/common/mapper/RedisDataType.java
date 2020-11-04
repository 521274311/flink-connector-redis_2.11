package club.kingon.flink.connectors.redis.common.mapper;

/**
 * @author dragons
 * @date 2020/11/4 11:28
 */
public enum  RedisDataType {
    STRING,
    HASH,
    LIST,
    SET,
    SORTED_SET,
    HYPER_LOG_LOG,
    PUBSUB;

    private RedisDataType() {
    }
}
