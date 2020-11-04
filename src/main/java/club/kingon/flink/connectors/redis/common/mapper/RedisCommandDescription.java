package club.kingon.flink.connectors.redis.common.mapper;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * @author dragons
 * @date 2020/11/4 11:27
 */
public class RedisCommandDescription implements Serializable {
    private static final long serialVersionUID = 1L;
    private RedisCommand redisCommand;

    public RedisCommandDescription(RedisCommand redisCommand) {
        Preconditions.checkNotNull(redisCommand, "Redis command type can not be null");
        this.redisCommand = redisCommand;
    }

    public RedisCommand getCommand() {
        return this.redisCommand;
    }
}
