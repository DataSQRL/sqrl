package com.datasqrl.engine.database.relational;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.tables.FlinkTableBuilder;

import lombok.Value;

/**
 * For the JDBC database engines, we just keep track of the
 * schema data so we can plan them later in the plan method.
 */
@Value
public class JdbcEngineCreateTable implements EngineCreateTable {

  FlinkTableBuilder table;
  RelDataType datatype;
  TableAnalysis tableAnalysis;

}
