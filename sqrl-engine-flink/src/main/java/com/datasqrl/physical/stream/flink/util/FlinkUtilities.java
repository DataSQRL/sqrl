package com.datasqrl.physical.stream.flink.util;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class FlinkUtilities {

  public static final Random random = new Random();

  public static int generateBalancedKey(final int parallelism) {
    return random.nextInt(parallelism << 8);
  }

  public static <T> KeySelector<T, Integer> getSingleKeySelector(final int key) {
    return new KeySelector<T, Integer>() {
      @Override
      public Integer getKey(T t) throws Exception {
        return key;
      }
    };
  }

  public static long getCurrentProcessingTime() {
    return System.currentTimeMillis();
  }

  public static void enableCheckpointing(StreamExecutionEnvironment env) {
    env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1), CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
  }

  public static <T> KeySelector<T, Integer> getHashPartitioner(final int parallelism) {
    final int modulus = parallelism;
    return new KeySelector<T, Integer>() {
      @Override
      public Integer getKey(T sourceRecord) throws Exception {
        return sourceRecord.hashCode() % modulus;
      }
    };
  }

}
