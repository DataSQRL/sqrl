package com.datasqrl.config;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import lombok.NonNull;

public class SqrlConfigUtil {

  public static Map<String, String> toStringMap(@NonNull SqrlConfig config, @NonNull Collection<String> withoutKeys) {
    return toMap(config, Object::toString, withoutKeys);
  }

  public static<R> Map<String, R> toMap(@NonNull SqrlConfig config, Function<Object,R> valueFunction, @NonNull Collection<String> withoutKeys) {
    var result = new LinkedHashMap<String, R>();
    config.toMap().forEach((key,value) -> {
      if (withoutKeys.contains(key) || key.equals(SqrlConfig.VERSION_KEY)) {
		return;
	}
      result.put(key,valueFunction.apply(value));
    });
    return result;
  }

  public static Properties toProperties(@NonNull SqrlConfig config, @NonNull Collection<String> withoutKeys) {
    var prop = new Properties();
    prop.putAll(toMap(config,Function.identity(),withoutKeys));
    return prop;
  }


}
