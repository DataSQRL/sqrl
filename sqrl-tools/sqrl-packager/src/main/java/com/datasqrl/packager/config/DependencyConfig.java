package com.datasqrl.packager.config;

import com.datasqrl.config.SqrlConfig;
import java.util.LinkedHashMap;
import lombok.NonNull;

public class DependencyConfig {

  public static final String DEPENDENCIES_KEY = "dependencies";

  public static LinkedHashMap<String,Dependency> fromRootConfig(@NonNull SqrlConfig config) {
    return config.asMap(DEPENDENCIES_KEY,Dependency.class).get();
  }

}
