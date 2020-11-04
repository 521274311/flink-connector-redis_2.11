package club.kingon.flink.connectors.redis.common.mapper.impl;

import club.kingon.flink.connectors.redis.common.mapper.RedisCommand;
import club.kingon.flink.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author dragons
 * @date 2020/11/4 13:36
 */
public class DefaultRedisSetMapper extends BaseRedisMapper<Tuple2<String, String>> {

    public DefaultRedisSetMapper() {
        super();
    }

    public DefaultRedisSetMapper(int expirationSecond) {
        super(expirationSecond);
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SADD);
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> value) {
        return value.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, String> value) {
        return value.f1;
    }
}
