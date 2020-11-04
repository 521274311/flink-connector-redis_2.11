package club.kingon.flink.connectors.redis.common.mapper.impl;


import club.kingon.flink.connectors.redis.common.mapper.RedisMapper;

/**
 * @author dragons
 * @date 2020/11/4 13:18
 */
public abstract class BaseRedisMapper<T> implements RedisMapper<T> {
    private static final long serialVersionUID = 1L;
    private final int expirationSecond;

    public BaseRedisMapper() {
        super();
        expirationSecond = -1;
    }

    public BaseRedisMapper(int expirationSecond) {
        this.expirationSecond = expirationSecond;
    }

    @Override
    public int getExpirationSecond() {
        return expirationSecond;
    }
}
