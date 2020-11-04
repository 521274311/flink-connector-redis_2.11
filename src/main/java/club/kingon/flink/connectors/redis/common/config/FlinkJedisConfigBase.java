package club.kingon.flink.connectors.redis.common.config;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * <p>Flink Jedis Config Base Class</p>
 * @author dragons
 * @date 2020/11/4 11:32
 */
public class FlinkJedisConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final int maxTotal;
    protected final int maxIdle;
    protected final int minIdle;
    protected final int connectionTimeout;

    protected FlinkJedisConfigBase(int connectionTimeout, int maxTotal, int maxIdle, int minIdle) {
        Preconditions.checkArgument(connectionTimeout >= 0, "connection timeout can not be negative");
        Preconditions.checkArgument(maxTotal >= 0, "maxTotal value can not be negative");
        Preconditions.checkArgument(maxIdle >= 0, "maxIdle value can not be negative");
        Preconditions.checkArgument(minIdle >= 0, "minIdle value can not be negative");
        this.connectionTimeout = connectionTimeout;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.minIdle = minIdle;
    }

    public int getConnectionTimeout() {
        return this.connectionTimeout;
    }

    public int getMaxTotal() {
        return this.maxTotal;
    }

    public int getMaxIdle() {
        return this.maxIdle;
    }

    public int getMinIdle() {
        return this.minIdle;
    }
}
