package com.datasqrl.engine.log;

import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.engine.export.ExportEngine;
import com.datasqrl.plan.global.IndexSelectorConfig;

public interface LogEngine extends ExportEngine, DatabaseEngine {

  @Deprecated
  LogFactory getLogFactory();

  @Override
  @Deprecated
  default IndexSelectorConfig getIndexSelectorConfig() {
    throw new UnsupportedOperationException("Deprecated - should not be called");
  }

  @Override
  default boolean supportsQueryEngine(QueryEngine engine) {
    return false;
  }

  @Override
  default void addQueryEngine(QueryEngine engine) {
    throw new UnsupportedOperationException("Not supported");
  }
}
