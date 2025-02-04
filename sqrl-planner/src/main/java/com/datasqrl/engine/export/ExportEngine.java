package com.datasqrl.engine.export;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import org.apache.calcite.rel.type.RelDataType;

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
   * @return
   */
  default EngineCreateTable createTable(ExecutionStage stage, String originalTableName,
      FlinkTableBuilder tableBuilder, RelDataType relDataType) {
    return null;
  }

  default DataTypeMapping getTypeMapping() {
    return DataTypeMapping.NONE;
  }

  default EnginePhysicalPlan plan(MaterializationStagePlan stagePlan) {
    return null;
  }


}
