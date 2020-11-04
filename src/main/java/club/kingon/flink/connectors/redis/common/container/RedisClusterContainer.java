package club.kingon.flink.connectors.redis.common.container;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author dragons
 * @date 2020/11/4 11:36
 */
public class RedisClusterContainer implements RedisCommandsContainer, Closeable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterContainer.class);
    private JedisCluster jedisCluster;

    public RedisClusterContainer(JedisCluster jedisCluster) {
        Preconditions.checkNotNull(jedisCluster, "Jedis cluster can not be null");
        this.jedisCluster = jedisCluster;
    }

    @Override
    public void hset(String key, String hashField, String value) {
        try {
            this.jedisCluster.hset(key, hashField, value);
        } catch (Exception var5) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HSET to hash {} error message {}", key, hashField, var5.getMessage());
            }

            throw var5;
        }
    }

    @Override
    public void rpush(String listName, String value) {
        try {
            this.jedisCluster.rpush(listName, value);
        } catch (Exception var4) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command RPUSH to list {} error message: {}", listName, var4.getMessage());
            }

            throw var4;
        }
    }

    @Override
    public void lpush(String listName, String value) {
        try {
            this.jedisCluster.lpush(listName, value);
        } catch (Exception var4) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command LPUSH to list {} error message: {}", listName, var4.getMessage());
            }

            throw var4;
        }
    }

    @Override
    public void sadd(String setName, String value) {
        try {
            this.jedisCluster.sadd(setName, value);
        } catch (Exception var4) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command RPUSH to set {} error message {}", setName, var4.getMessage());
            }

            throw var4;
        }
    }

    @Override
    public void publish(String channelName, String message) {
        try {
            this.jedisCluster.publish(channelName, message);
        } catch (Exception var4) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PUBLISH to channel {} error message {}", channelName, var4.getMessage());
            }

            throw var4;
        }
    }

    @Override
    public void set(String key, String value) {
        try {
            this.jedisCluster.set(key, value);
        } catch (Exception var4) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SET to key {} error message {}", key, var4.getMessage());
            }

            throw var4;
        }
    }

    @Override
    public void pfadd(String key, String element) {
        try {
            this.jedisCluster.set(key, element);
        } catch (Exception var4) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PFADD to key {} error message {}", key, var4.getMessage());
            }

            throw var4;
        }
    }

    @Override
    public void zadd(String key, String score, String element) {
        try {
            this.jedisCluster.zadd(key, Double.valueOf(score), element);
        } catch (Exception var5) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command ZADD to set {} error message {}", key, var5.getMessage());
            }

            throw var5;
        }
    }

    @Override
    public void expire(String key, int second) {
        try {
            this.jedisCluster.expire(key, second);
        } catch (Exception var6) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command EXPIRE to set {} error message {}", key, var6.getMessage());
            }
            throw var6;
        }

    }

    @Override
    public void close() throws IOException {
        this.jedisCluster.close();
    }
}
