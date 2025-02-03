package com.datasqrl.engine;

import com.datasqrl.engine.pipeline.ExecutionStage;

/**
 * Represents a query that can be executed against it's {@link ExecutionStage}
 * to execute the associated {@link com.datasqrl.v2.tables.SqrlTableFunction}.
 */
public interface ExecutableQuery {

  ExecutionStage getStage();

}
