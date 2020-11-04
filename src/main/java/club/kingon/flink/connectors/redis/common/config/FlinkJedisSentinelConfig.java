package club.kingon.flink.connectors.redis.common.config;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * <p>Flink Jedis Sentinel Config Class</p>
 * @author dragons
 * @date 2020/11/4 11:36
 */
public class FlinkJedisSentinelConfig extends FlinkJedisConfigBase {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJedisSentinelConfig.class);
    private final String masterName;
    private final Set<String> sentinels;
    private final int soTimeout;
    private final String password;
    private final int database;

    private FlinkJedisSentinelConfig(String masterName, Set<String> sentinels, int connectionTimeout, int soTimeout, String password, int database, int maxTotal, int maxIdle, int minIdle) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle);
        Preconditions.checkNotNull(masterName, "Master name should be presented");
        Preconditions.checkNotNull(sentinels, "Sentinels information should be presented");
        Preconditions.checkArgument(!sentinels.isEmpty(), "Sentinel hosts should not be empty");
        this.masterName = masterName;
        this.sentinels = new HashSet<>(sentinels);
        this.soTimeout = soTimeout;
        this.password = password;
        this.database = database;
    }

    public String getMasterName() {
        return this.masterName;
    }

    public Set<String> getSentinels() {
        return this.sentinels;
    }

    public int getSoTimeout() {
        return this.soTimeout;
    }

    public String getPassword() {
        return this.password;
    }

    public int getDatabase() {
        return this.database;
    }

    @Override
    public String toString() {
        return "JedisSentinelConfig{masterName='" + this.masterName + '\'' + ", connectionTimeout=" + this.connectionTimeout + ", soTimeout=" + this.soTimeout + ", database=" + this.database + ", maxTotal=" + this.maxTotal + ", maxIdle=" + this.maxIdle + ", minIdle=" + this.minIdle + '}';
    }

    public static class Builder {
        private String masterName;
        private Set<String> sentinels;
        private int connectionTimeout = 2000;
        private int soTimeout = 2000;
        private String password;
        private int database = 0;
        private int maxTotal = 8;
        private int maxIdle = 8;
        private int minIdle = 0;

        public Builder() {
        }

        public FlinkJedisSentinelConfig.Builder setMasterName(String masterName) {
            this.masterName = masterName;
            return this;
        }

        public FlinkJedisSentinelConfig.Builder setSentinels(Set<String> sentinels) {
            this.sentinels = sentinels;
            return this;
        }

        public FlinkJedisSentinelConfig.Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public FlinkJedisSentinelConfig.Builder setSoTimeout(int soTimeout) {
            this.soTimeout = soTimeout;
            return this;
        }

        public FlinkJedisSentinelConfig.Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public FlinkJedisSentinelConfig.Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public FlinkJedisSentinelConfig.Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public FlinkJedisSentinelConfig.Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        public FlinkJedisSentinelConfig.Builder setMinIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public FlinkJedisSentinelConfig build() {
            return new FlinkJedisSentinelConfig(this.masterName, this.sentinels, this.connectionTimeout, this.soTimeout, this.password, this.database, this.maxTotal, this.maxIdle, this.minIdle);
        }
    }
}
