package club.kingon.flink.connectors.redis.common.mapper.impl;

import club.kingon.flink.connectors.redis.common.mapper.RedisCommand;
import club.kingon.flink.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author dragons
 * @date 2020/11/4 13:40
 */
public class DefaultRedisZSetMapper extends BaseRedisMapper<Tuple3<String, String, String>> {

    public DefaultRedisZSetMapper() {
        super();
    }

    public DefaultRedisZSetMapper(int expirationSecond) {
        super(expirationSecond);
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.ZADD);
    }

    @Override
    public String getKeyFromData(Tuple3<String, String, String> value) {
        return value.f0;
    }

    @Override
    public String getValueFromData(Tuple3<String, String, String> value) {
        return value.f2;
    }

    @Override
    public String getFieldFromData(Tuple3<String, String, String> value) {
        return value.f1;
    }
}
