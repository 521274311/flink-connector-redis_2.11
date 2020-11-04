package club.kingon.flink.connectors.redis.common.mapper.impl;

import club.kingon.flink.connectors.redis.common.mapper.RedisCommand;
import club.kingon.flink.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * <p>Redis Hash Mapper</p>
 * @author dragons
 * @date 2020/11/4 13:24
 */
public class DefaultRedisHashMapper extends BaseRedisMapper<Tuple3<String, String, String>> {

    public DefaultRedisHashMapper() {
        super();
    }

    public DefaultRedisHashMapper(int expirationSecond) {
        super(expirationSecond);
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET);
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
