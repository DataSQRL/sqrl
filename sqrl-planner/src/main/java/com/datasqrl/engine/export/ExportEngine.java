package com.datasqrl.engine.export;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.flinkwrapper.tables.FlinkTableBuilder;
import org.apache.calcite.rel.type.RelDataType;

public interface ExportEngine extends ExecutionEngine {

  EngineCreateTable createTable(FlinkTableBuilder tableBuilder, RelDataType relDataType);

  DataTypeMapping getTypeMapping();

}
