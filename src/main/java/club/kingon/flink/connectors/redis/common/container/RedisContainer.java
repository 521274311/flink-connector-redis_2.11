package club.kingon.flink.connectors.redis.common.container;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author dragons
 * @date 2020/11/4 11:40
 */
public class RedisContainer implements RedisCommandsContainer, Closeable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);
    private final JedisPool jedisPool;
    private final JedisSentinelPool jedisSentinelPool;

    public RedisContainer(JedisPool jedisPool) {
        Preconditions.checkNotNull(jedisPool, "Jedis Pool can not be null");
        this.jedisPool = jedisPool;
        this.jedisSentinelPool = null;
    }

    public RedisContainer(JedisSentinelPool sentinelPool) {
        Preconditions.checkNotNull(sentinelPool, "Jedis Sentinel Pool can not be null");
        this.jedisPool = null;
        this.jedisSentinelPool = sentinelPool;
    }

    @Override
    public void close() throws IOException {
        if (this.jedisPool != null) {
            this.jedisPool.close();
        }

        if (this.jedisSentinelPool != null) {
            this.jedisSentinelPool.close();
        }

    }

    @Override
    public void hset(String key, String hashField, String value) {
        Jedis jedis = null;

        try {
            jedis = this.getInstance();
            jedis.hset(key, hashField, value);
        } catch (Exception var9) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HSET to key {} and hashField {} error message {}", new Object[]{key, hashField, var9.getMessage()});
            }

            throw var9;
        } finally {
            this.releaseInstance(jedis);
        }

    }

    @Override
    public void rpush(String listName, String value) {
        Jedis jedis = null;

        try {
            jedis = this.getInstance();
            jedis.rpush(listName, value);
        } catch (Exception var8) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command RPUSH to list {} error message {}", listName, var8.getMessage());
            }

            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }

    }

    @Override
    public void lpush(String listName, String value) {
        Jedis jedis = null;

        try {
            jedis = this.getInstance();
            jedis.lpush(listName, value);
        } catch (Exception var8) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command LUSH to list {} error message {}", listName, var8.getMessage());
            }

            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }

    }

    @Override
    public void sadd(String setName, String value) {
        Jedis jedis = null;

        try {
            jedis = this.getInstance();
            jedis.sadd(setName, value);
        } catch (Exception var8) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command RPUSH to set {} error message {}", setName, var8.getMessage());
            }

            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }

    }

    @Override
    public void publish(String channelName, String message) {
        Jedis jedis = null;

        try {
            jedis = this.getInstance();
            jedis.publish(channelName, message);
        } catch (Exception var8) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PUBLISH to channel {} error message {}", channelName, var8.getMessage());
            }

            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }

    }

    @Override
    public void set(String key, String value) {
        Jedis jedis = null;

        try {
            jedis = this.getInstance();
            jedis.set(key, value);
        } catch (Exception var8) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SET to key {} error message {}", key, var8.getMessage());
            }

            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }

    }

    @Override
    public void pfadd(String key, String element) {
        Jedis jedis = null;

        try {
            jedis = this.getInstance();
            jedis.pfadd(key, element);
        } catch (Exception var8) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PFADD to key {} error message {}", key, var8.getMessage());
            }

            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }

    }

    @Override
    public void zadd(String key, String score, String element) {
        Jedis jedis = null;

        try {
            jedis = this.getInstance();
            jedis.zadd(key, Double.valueOf(score), element);
        } catch (Exception var9) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command ZADD to set {} error message {}", key, var9.getMessage());
            }

            throw var9;
        } finally {
            this.releaseInstance(jedis);
        }

    }

    @Override
    public void expire(String key, int second) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.expire(key, second);
        } catch (Exception var10) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command EXPIRE to set {} error message {}", key, var10.getMessage());
            }
            throw var10;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    private Jedis getInstance() {
        return this.jedisSentinelPool != null ? this.jedisSentinelPool.getResource() : this.jedisPool.getResource();
    }

    private void releaseInstance(Jedis jedis) {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception var3) {
                LOG.error("Failed to close (return) instance to pool", var3);
            }

        }
    }
}
