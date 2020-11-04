package club.kingon.flink.connectors.redis.common.config;

import org.apache.flink.util.Preconditions;
import redis.clients.jedis.HostAndPort;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>Flink Jedis Cluster Config Class</p>
 * @author dragons
 * @date 2020/11/4 11:35
 */
public class FlinkJedisClusterConfig extends FlinkJedisConfigBase {
    private static final long serialVersionUID = 1L;
    private final Set<InetSocketAddress> nodes;
    private final int maxRedirections;

    private FlinkJedisClusterConfig(Set<InetSocketAddress> nodes, int connectionTimeout, int maxRedirections, int maxTotal, int maxIdle, int minIdle) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle);
        Preconditions.checkNotNull(nodes, "Node information should be presented");
        Preconditions.checkArgument(!nodes.isEmpty(), "Redis cluster hosts should not be empty");
        this.nodes = new HashSet<>(nodes);
        this.maxRedirections = maxRedirections;
    }

    public Set<HostAndPort> getNodes() {
        Set<HostAndPort> ret = new HashSet<>();

        for (InetSocketAddress node : this.nodes) {
            ret.add(new HostAndPort(node.getHostName(), node.getPort()));
        }

        return ret;
    }

    public int getMaxRedirections() {
        return this.maxRedirections;
    }

    @Override
    public String toString() {
        return "JedisClusterConfig{nodes=" + this.nodes + ", timeout=" + this.connectionTimeout + ", maxRedirections=" + this.maxRedirections + ", maxTotal=" + this.maxTotal + ", maxIdle=" + this.maxIdle + ", minIdle=" + this.minIdle + '}';
    }

    public static class Builder {
        private Set<InetSocketAddress> nodes;
        private int timeout = 2000;
        private int maxRedirections = 5;
        private int maxTotal = 8;
        private int maxIdle = 8;
        private int minIdle = 0;

        public Builder() {
        }

        public FlinkJedisClusterConfig.Builder setNodes(Set<InetSocketAddress> nodes) {
            this.nodes = nodes;
            return this;
        }

        public FlinkJedisClusterConfig.Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public FlinkJedisClusterConfig.Builder setMaxRedirections(int maxRedirections) {
            this.maxRedirections = maxRedirections;
            return this;
        }

        public FlinkJedisClusterConfig.Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public FlinkJedisClusterConfig.Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        public FlinkJedisClusterConfig.Builder setMinIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public FlinkJedisClusterConfig build() {
            return new FlinkJedisClusterConfig(this.nodes, this.timeout, this.maxRedirections, this.maxTotal, this.maxIdle, this.minIdle);
        }
    }
}
