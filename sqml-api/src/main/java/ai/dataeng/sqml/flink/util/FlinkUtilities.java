package ai.dataeng.sqml.flink.util;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.Random;

public class FlinkUtilities {

    public static final Random random = new Random();

    public static int generateBalancedKey(int parallelism) {
        return random.nextInt(parallelism<<8);
    }

    public static<T> KeySelector<T,Integer> getSingleKeySelector(final int key) {
        return new KeySelector<T, Integer>() {
            @Override
            public Integer getKey(T t) throws Exception {
                return key;
            }
        };
    }


}
