package com.datasqrl.engine.export;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import org.apache.calcite.rel.type.RelDataType;

public interface ExportEngine extends ExecutionEngine {

  default EngineCreateTable createTable(ExecutionStage stage, String tableName, FlinkTableBuilder tableBuilder, RelDataType relDataType) {
    return null;
  }

  default DataTypeMapping getTypeMapping() {
    return null;
  }

  default EnginePhysicalPlan plan(MaterializationStagePlan stagePlan) {
    return null;
  }


}
