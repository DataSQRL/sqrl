/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import com.datasqrl.config.SqrlConfig;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.*;

import java.util.Optional;

public class ScriptConfiguration {

  public static final String SCRIPT_KEY = "script";
  public static final String MAIN_KEY = "main";
  public static final String GRAPHQL_KEY = "graphql";
  public static final String GRAPHQL_NORMALIZED_FILE_NAME = "schema.graphqls";

  public static final String[] FILE_KEYS = {MAIN_KEY, GRAPHQL_KEY};
  public static final Map<String,Optional<String>> NORMALIZED_FILE_NAMES = ImmutableMap.of(MAIN_KEY,
      Optional.empty(), GRAPHQL_KEY, Optional.of(GRAPHQL_NORMALIZED_FILE_NAME));

  public static SqrlConfig fromScriptConfig(@NonNull SqrlConfig rootConfig) {
    return rootConfig.getSubConfig(SCRIPT_KEY);
  }

  public static Map<String,Optional<String>> getFiles(@NonNull SqrlConfig rootConfig) {
    SqrlConfig config = fromScriptConfig(rootConfig);
    return Arrays.stream(FILE_KEYS).collect(Collectors.toMap(Function.identity(),
        fileKey -> config.asString(fileKey).getOptional()));
  }

}
