package club.kingon.flink.connectors.redis.example;

import club.kingon.flink.connectors.redis.RedisSink;
import club.kingon.flink.connectors.redis.common.config.FlinkJedisPoolConfig;
import club.kingon.flink.connectors.redis.common.mapper.impl.DefaultRedisSetMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * <p>Redis Set Demo</p>
 * @author dragons
 * @date 2020/11/4 16:41
 */
public class RedisSetExample {
    private final static String HOST = "xxx";
    private final static int PORT = 6379;
    private final static String PASSWORD = "xxx";
    private final static int DATABASE = 0;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple2<String, String>> stream = env.fromCollection(Arrays.asList(
                Tuple2.of("key1", "20201123"),
                Tuple2.of("key2", "20201022"),
                Tuple2.of("key3", "20201222"),
                Tuple2.of("key4", "20200512")
        ));
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(HOST)
                .setPort(PORT)
                .setPassword(PASSWORD)
                .setDatabase(DATABASE)
                .build();
        stream.addSink(new RedisSink<>(conf, new DefaultRedisSetMapper(600)));
        env.execute("test_redis");
    }
}
