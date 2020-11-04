package club.kingon.flink.connectors.redis;

import club.kingon.flink.connectors.redis.common.config.FlinkJedisConfigBase;
import club.kingon.flink.connectors.redis.common.container.RedisCommandsContainer;
import club.kingon.flink.connectors.redis.common.container.RedisCommandsContainerBuilder;
import club.kingon.flink.connectors.redis.common.mapper.RedisCommand;
import club.kingon.flink.connectors.redis.common.mapper.RedisCommandDescription;
import club.kingon.flink.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dragons
 * @date 2020/11/4 11:25
 */
public class RedisSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    private RedisMapper<IN> redisSinkMapper;
    private RedisCommand redisCommand;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    public RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper) {
        Preconditions.checkNotNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");
        Preconditions.checkNotNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null");
        this.flinkJedisConfigBase = flinkJedisConfigBase;
        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription = redisSinkMapper.getCommandDescription();
        this.redisCommand = redisCommandDescription.getCommand();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
    }

    @Override
    public void close() throws Exception {
        if (this.redisCommandsContainer != null) {
            this.redisCommandsContainer.close();
        }
    }

    @Override
    public void invoke(IN input) throws Exception {
        String key = this.redisSinkMapper.getKeyFromData(input);
        String field = this.redisSinkMapper.getFieldFromData(input);
        String value = this.redisSinkMapper.getValueFromData(input);
        int expirationSecond = this.redisSinkMapper.getExpirationSecond();
        switch (this.redisCommand) {
            case RPUSH:
                this.redisCommandsContainer.rpush(key, value, expirationSecond);
                break;
            case LPUSH:
                this.redisCommandsContainer.lpush(key, value, expirationSecond);
                break;
            case SADD:
                this.redisCommandsContainer.sadd(key, value, expirationSecond);
                break;
            case SET:
                this.redisCommandsContainer.set(key, value, expirationSecond);
                break;
            case PFADD:
                this.redisCommandsContainer.pfadd(key, value, expirationSecond);
                break;
            case PUBLISH:
                this.redisCommandsContainer.publish(key, value, expirationSecond);
                break;
            case ZADD:
                this.redisCommandsContainer.zadd(key, value, field, expirationSecond);
                break;
            case HSET:
                this.redisCommandsContainer.hset(key, field, value, expirationSecond);
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + this.redisCommand);
        }
    }
}
