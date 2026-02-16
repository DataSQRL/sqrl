package com.datasqrl.engine.log;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.graphql.server.MutationInsertType;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.time.Duration;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

public interface MutationEngine extends ExecutionEngine {

  /**
   * Create a new mutation table in the project
   *
   * @param stage
   * @param originalTableName
   * @param tableBuilder
   * @param relDataType
   * @param insertType
   * @param ttl
   * @return
   */
  default MutationCreateTable createMutation(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      MutationInsertType insertType,
      Optional<Duration> ttl) {
    throw new UnsupportedOperationException("Does not support mutations");
  }

  interface MutationCreateTable extends EngineCreateTable {
    MutationCreateTable withValueType(RelDataType inputValueType);
  }
}
