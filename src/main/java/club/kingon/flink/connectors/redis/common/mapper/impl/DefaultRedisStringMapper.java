package club.kingon.flink.connectors.redis.common.mapper.impl;

import club.kingon.flink.connectors.redis.common.mapper.RedisCommand;
import club.kingon.flink.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * <p>Redis String Mapper</p>
 * @author dragons
 * @date 2020/11/4 13:15
 */
public class DefaultRedisStringMapper extends BaseRedisMapper<Tuple2<String, String>> {

    public DefaultRedisStringMapper() {
        super();
    }

    public DefaultRedisStringMapper(int expirationSecond) {
        super(expirationSecond);
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
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
