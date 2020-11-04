package club.kingon.flink.connectors.redis.common.mapper;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * @author dragons
 * @date 2020/11/4 11:26
 */
public interface RedisMapper<T> extends Function, Serializable {
    RedisCommandDescription getCommandDescription();

    String getKeyFromData(T var1);

    String getValueFromData(T var1);

    /**
     * Hash and Sorted Set may be used.
     */
    default String getFieldFromData(T val1) {
        return null;
    }

    /**
     * All Keys expired second.
     */
    default int getExpirationSecond() {
        return -1;
    }
}
