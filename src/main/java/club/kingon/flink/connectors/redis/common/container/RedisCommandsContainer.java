package club.kingon.flink.connectors.redis.common.container;

import java.io.IOException;
import java.io.Serializable;

public interface RedisCommandsContainer extends Serializable {
    void hset(String var1, String var2, String var3);

    void rpush(String var1, String var2);

    void lpush(String var1, String var2);

    void sadd(String var1, String var2);

    void publish(String var1, String var2);

    void set(String var1, String var2);

    void pfadd(String var1, String var2);

    void zadd(String var1, String var2, String var3);

    void close() throws IOException;

    default void expire(String val1, int second) {
    }

    default void hset(String var1, String var2, String var3, int second) {
        hset(var1, var2, var3);
        expire(var1, second);
    }

    default void rpush(String var1, String var2, int second) {
        rpush(var1, var2);
        expire(var1, second);
    }

    default void lpush(String var1, String var2, int second) {
        lpush(var1, var2);
        expire(var1, second);
    }

    default void sadd(String var1, String var2, int second) {
        sadd(var1, var2);
        expire(var1, second);
    }

    default void publish(String var1, String var2, int second) {
        publish(var1, var2);
        expire(var1, second);
    }

    default void set(String var1, String var2, int second) {
        set(var1, var2);
        expire(var1, second);
    }

    default void pfadd(String var1, String var2, int second) {
        pfadd(var1, var2);
        expire(var1, second);
    }

    default void zadd(String var1, String var2, String var3, int second) {
        zadd(var1, var2, var3);
        expire(var1, second);
    }
    
}
