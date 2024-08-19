package com.datasqrl.engine.database;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.plan.queries.IdentifiedQuery;
import java.util.Map;

/**
 * A {@link QueryEngine} executes queries against a {@link DatabaseEngine} that supports the query
 * engine. A query engine does not persist data but only processes data stored elsewhere to produce
 * query results.
 */
public interface QueryEngine extends ExecutionEngine {


  default Map<IdentifiedQuery, QueryTemplate> updateQueries(
      ConnectorFactoryFactory connectorFactory, EngineConfig connectorConfig, Map<IdentifiedQuery, QueryTemplate> queries) {
    return queries;
  }
}
