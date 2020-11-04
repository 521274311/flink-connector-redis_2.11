package club.kingon.flink.connectors.redis.common.container;

import club.kingon.flink.connectors.redis.common.config.FlinkJedisClusterConfig;
import club.kingon.flink.connectors.redis.common.config.FlinkJedisConfigBase;
import club.kingon.flink.connectors.redis.common.config.FlinkJedisPoolConfig;
import club.kingon.flink.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.util.Preconditions;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

/**
 * @author dragons
 * @date 2020/11/4 11:39
 */
public class RedisCommandsContainerBuilder {
    public RedisCommandsContainerBuilder() {
    }

    public static RedisCommandsContainer build(FlinkJedisConfigBase flinkJedisConfigBase) {
        if (flinkJedisConfigBase instanceof FlinkJedisPoolConfig) {
            FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig)flinkJedisConfigBase;
            return build(flinkJedisPoolConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisClusterConfig) {
            FlinkJedisClusterConfig flinkJedisClusterConfig = (FlinkJedisClusterConfig)flinkJedisConfigBase;
            return build(flinkJedisClusterConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisSentinelConfig) {
            FlinkJedisSentinelConfig flinkJedisSentinelConfig = (FlinkJedisSentinelConfig)flinkJedisConfigBase;
            return build(flinkJedisSentinelConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    public static RedisCommandsContainer build(FlinkJedisPoolConfig jedisPoolConfig) {
        Preconditions.checkNotNull(jedisPoolConfig, "Redis pool config should not be Null");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());
        JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, jedisPoolConfig.getHost(), jedisPoolConfig.getPort(), jedisPoolConfig.getConnectionTimeout(), jedisPoolConfig.getPassword(), jedisPoolConfig.getDatabase());
        return new RedisContainer(jedisPool);
    }

    public static RedisCommandsContainer build(FlinkJedisClusterConfig jedisClusterConfig) {
        Preconditions.checkNotNull(jedisClusterConfig, "Redis cluster config should not be Null");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisClusterConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisClusterConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisClusterConfig.getMinIdle());
        JedisCluster jedisCluster = new JedisCluster(jedisClusterConfig.getNodes(), jedisClusterConfig.getConnectionTimeout(), jedisClusterConfig.getMaxRedirections(), genericObjectPoolConfig);
        return new RedisClusterContainer(jedisCluster);
    }

    public static RedisCommandsContainer build(FlinkJedisSentinelConfig jedisSentinelConfig) {
        Preconditions.checkNotNull(jedisSentinelConfig, "Redis sentinel config should not be Null");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisSentinelConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisSentinelConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisSentinelConfig.getMinIdle());
        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(jedisSentinelConfig.getMasterName(), jedisSentinelConfig.getSentinels(), genericObjectPoolConfig, jedisSentinelConfig.getConnectionTimeout(), jedisSentinelConfig.getSoTimeout(), jedisSentinelConfig.getPassword(), jedisSentinelConfig.getDatabase());
        return new RedisContainer(jedisSentinelPool);
    }
}
