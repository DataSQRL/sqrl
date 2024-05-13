package com.datasqrl.config;

import static com.datasqrl.config.PackageJsonImpl.SCRIPT_KEY;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
public class ScriptConfigImpl implements PackageJson.ScriptConfig {

  SqrlConfig sqrlConfig;


  public static final String MAIN_KEY = "main";
  public static final String GRAPHQL_KEY = "graphql";
  public static final String GRAPHQL_NORMALIZED_FILE_NAME = "schema.graphqls";

  public static final String[] FILE_KEYS = {MAIN_KEY, GRAPHQL_KEY};

  public static final Map<String, Optional<String>> NORMALIZED_FILE_NAMES = ImmutableMap.of(MAIN_KEY,
      Optional.empty(), GRAPHQL_KEY, Optional.of(GRAPHQL_NORMALIZED_FILE_NAME));

  public static PackageJson.ScriptConfig fromScriptConfig(@NonNull SqrlConfig rootConfig) {
    return new ScriptConfigImpl(rootConfig.getSubConfig(SCRIPT_KEY));
  }

//  public static Map<String,Optional<String>> getFiles(@NonNull SqrlConfig rootConfig) {
//    ScriptConfig config = fromScriptConfig(rootConfig);
//    return Arrays.stream(FILE_KEYS).collect(Collectors.toMap(Function.identity(),
//        fileKey -> config.asString(fileKey).getOptional()));
//  }

  @Override
  public Optional<String> getMainScript() {
    return sqrlConfig.asString(MAIN_KEY).getOptional();
  }

  @Override
  public Optional<String>  getGraphql() {
    return sqrlConfig.asString(GRAPHQL_KEY).getOptional();
  }

  @Override
  public void setMainScript(String script) {
    sqrlConfig.setProperty(MAIN_KEY, script);
  }

  @Override
  public void setGraphql(String graphql) {
    sqrlConfig.setProperty(GRAPHQL_KEY, graphql);
  }
}
