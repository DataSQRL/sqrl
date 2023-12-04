package com.datasqrl.engine.log;

import com.datasqrl.calcite.type.NamedRelDataType;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.datasqrl.schema.UniversalTable;

public interface LogEngine extends ExecutionEngine {

  Log createLog(String logId, NamedRelDataType schema);

}
