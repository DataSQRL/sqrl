package com.datasqrl.engine.export;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.tables.FlinkTableBuilder;

public interface ExportEngine extends ExecutionEngine {

  /**
   * Creates a table in the engine for the given execution stage.
   * Assume that the schema/columns have already been defined in the FlinkTableBuilder as well as the primary & partition key.
   * The engine should add connector options and watermarks.
   *
   * @param stage The execution stage for the engine
   * @param originalTableName The original name of the table. The actual table name might be different to make it unique.
   * @param tableBuilder The table builder
   * @param relDataType The datatype for the columns in the table.
   * @param tableAnalysis The table analysis for the table if this is a planned table (not available for mutations)
   * @return
   */
  EngineCreateTable createTable(ExecutionStage stage, String originalTableName,
      FlinkTableBuilder tableBuilder, RelDataType relDataType, Optional<TableAnalysis> tableAnalysis);

  DataTypeMapping getTypeMapping();




}
