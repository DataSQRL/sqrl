package org.apache.flink.table.api.internal;

import org.apache.flink.table.catalog.FunctionCatalog;

public class TableEnvExtractor {

  public static FunctionCatalog getFunctionCatalog(TableEnvironmentImpl env) {
    return env.functionCatalog;
  }
}
