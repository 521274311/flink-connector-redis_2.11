package club.kingon.flink.connectors.redis.example;

import club.kingon.flink.connectors.redis.RedisSink;
import club.kingon.flink.connectors.redis.common.config.FlinkJedisPoolConfig;
import club.kingon.flink.connectors.redis.common.mapper.impl.DefaultRedisHashMapper;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * <p>Redis Hash Demo</p>
 * @author dragons
 * @date 2020/11/4 16:36
 */
public class RedisHashExample {
    private final static String HOST = "xxx";
    private final static int PORT = 6379;
    private final static String PASSWORD = "xxx";
    private final static int DATABASE = 0;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple3<String, String, String>> stream = env.fromCollection(Arrays.asList(
                Tuple3.of("key1", "field1", "20201123"),
                Tuple3.of("key2", "field2", "20201022"),
                Tuple3.of("key3", "field3", "20201222"),
                Tuple3.of("key4", "field4", "20200512")
        ));
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(HOST)
                .setPort(PORT)
                .setPassword(PASSWORD)
                .setDatabase(DATABASE)
                .build();
        stream.addSink(new RedisSink<>(conf, new DefaultRedisHashMapper(600)));
        env.execute("test_redis");
    }
}
