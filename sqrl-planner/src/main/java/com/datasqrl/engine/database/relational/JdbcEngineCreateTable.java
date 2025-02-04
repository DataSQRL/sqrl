package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

/**
 * For the JDBC database engines, we just keep track of the
 * schema data so we can plan them later in the plan method.
 */
@Value
public class JdbcEngineCreateTable implements EngineCreateTable {

  FlinkTableBuilder table;
  RelDataType datatype;

}
