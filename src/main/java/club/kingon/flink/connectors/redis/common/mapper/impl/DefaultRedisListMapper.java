package club.kingon.flink.connectors.redis.common.mapper.impl;

import club.kingon.flink.connectors.redis.common.container.RedisClusterContainer;
import club.kingon.flink.connectors.redis.common.mapper.RedisCommand;
import club.kingon.flink.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Redis List Mapper</p>
 * @author dragons
 * @date 2020/11/4 13:27
 */
public class DefaultRedisListMapper extends BaseRedisMapper<Tuple2<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterContainer.class);
    private PushPosition position;

    public DefaultRedisListMapper(PushPosition position) {
        super();
        this.position = position;
    }

    public DefaultRedisListMapper(int expirationSecond, PushPosition position) {
        super(expirationSecond);
        this.position = position;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        switch (position) {
            case LEFT_PUSH:
                return new RedisCommandDescription(RedisCommand.LPUSH);
            case RIGHT_PUSH:
                return new RedisCommandDescription(RedisCommand.RPUSH);
            default:
                if (LOG.isErrorEnabled()) {
                    LOG.error("POSITION must be LEFT_PUSH or RIGHT_PUSH.");
                }
                return null;
        }
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> value) {
        return value.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, String> value) {
        return value.f1;
    }

    public enum PushPosition {
        LEFT_PUSH,
        RIGHT_PUSH;

        PushPosition() {
        }
    }
}
