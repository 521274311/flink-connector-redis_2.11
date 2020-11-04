package club.kingon.flink.connectors.redis.common.config;

import org.apache.flink.util.Preconditions;

/**
 * <p>Flink Jedis Pool Config Class</p>
 * @author dragons
 * @date 2020/11/4 11:36
 */
public class FlinkJedisPoolConfig extends FlinkJedisConfigBase {
    private static final long serialVersionUID = 1L;
    private final String host;
    private final int port;
    private final int database;
    private final String password;

    private FlinkJedisPoolConfig(String host, int port, int connectionTimeout, String password, int database, int maxTotal, int maxIdle, int minIdle) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle);
        Preconditions.checkNotNull(host, "Host information should be presented");
        this.host = host;
        this.port = port;
        this.database = database;
        this.password = password;
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public int getDatabase() {
        return this.database;
    }

    public String getPassword() {
        return this.password;
    }

    @Override
    public String toString() {
        return "JedisPoolConfig{host='" + this.host + '\'' + ", port=" + this.port + ", timeout=" + this.connectionTimeout + ", database=" + this.database + ", maxTotal=" + this.maxTotal + ", maxIdle=" + this.maxIdle + ", minIdle=" + this.minIdle + '}';
    }

    public static class Builder {
        private String host;
        private int port = 6379;
        private int timeout = 2000;
        private int database = 0;
        private String password;
        private int maxTotal = 8;
        private int maxIdle = 8;
        private int minIdle = 0;

        public Builder() {
        }

        public FlinkJedisPoolConfig.Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setMinIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public FlinkJedisPoolConfig build() {
            return new FlinkJedisPoolConfig(this.host, this.port, this.timeout, this.password, this.database, this.maxTotal, this.maxIdle, this.minIdle);
        }
    }
}
