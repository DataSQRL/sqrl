package com.datasqrl.config;

import java.util.*;
import java.util.function.Function;

import lombok.NonNull;

public class SqrlConfigUtil {

  public static Map<String, String> toStringMap(@NonNull SqrlConfig config, @NonNull Collection<String> withoutKeys) {
    return toMap(config, Object::toString, withoutKeys);
  }

  public static<R> Map<String, R> toMap(@NonNull SqrlConfig config, Function<Object,R> valueFunction, @NonNull Collection<String> withoutKeys) {
    LinkedHashMap<String, R> result = new LinkedHashMap<>();
    config.toMap().forEach((key,value) -> {
      if (withoutKeys.contains(key) || key.equals(SqrlConfig.VERSION_KEY)) return;
      result.put(key,valueFunction.apply(value));
    });
    return result;
  }

  public static Properties toProperties(@NonNull SqrlConfig config, @NonNull Collection<String> withoutKeys) {
    Properties prop = new Properties();
    prop.putAll(toMap(config,Function.identity(),withoutKeys));
    return prop;
  }


}
