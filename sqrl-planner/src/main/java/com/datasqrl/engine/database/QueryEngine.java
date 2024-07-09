package com.datasqrl.engine.database;

import com.datasqrl.engine.ExecutionEngine;

/**
 * A {@link QueryEngine} executes queries against a {@link DatabaseEngine} that supports the query
 * engine. A query engine does not persist data but only processes data stored elsewhere to produce
 * query results.
 */
public interface QueryEngine extends ExecutionEngine {



}
