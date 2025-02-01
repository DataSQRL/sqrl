package com.datasqrl.engine.export;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.flinkwrapper.dag.plan.MaterializationStagePlan;
import com.datasqrl.flinkwrapper.tables.FlinkTableBuilder;
import org.apache.calcite.rel.type.RelDataType;

public interface ExportEngine extends ExecutionEngine {

  EngineCreateTable createTable(ExecutionStage stage, FlinkTableBuilder tableBuilder, RelDataType relDataType);

  DataTypeMapping getTypeMapping();

  DatabasePhysicalPlan plan(MaterializationStagePlan stagePlan);


}
