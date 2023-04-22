package com.datasqrl.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lombok.NonNull;

public class SqrlConfigUtil {

  public static Map<String,String> toMap(@NonNull SqrlConfig config, @NonNull Collection<String> withoutKeys) {
    Map<String,String> conf = new HashMap<>();
    config.getAllKeys().forEach(key -> {
      if (!withoutKeys.contains(key)) conf.put(key, config.asString(key).get());
    });
    return conf;
  }

  public static Properties toProperties(@NonNull SqrlConfig config, @NonNull Collection<String> withoutKeys) {
    Properties prop = new Properties();
    prop.putAll(toMap(config,withoutKeys));
    return prop;
  }


}
