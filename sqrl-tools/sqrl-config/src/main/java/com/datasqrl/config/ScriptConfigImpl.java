package com.datasqrl.config;


import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ScriptConfigImpl implements PackageJson.ScriptConfig {

  SqrlConfig sqrlConfig;

  public static final String MAIN_KEY = "main";
  public static final String GRAPHQL_KEY = "graphql";
  public static final String GRAPHQL_NORMALIZED_FILE_NAME = "schema.graphqls";

  @Override
  public Optional<String> getMainScript() {
    return sqrlConfig.asString(MAIN_KEY).getOptional();
  }

  @Override
  public Optional<String> getGraphql() {
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
