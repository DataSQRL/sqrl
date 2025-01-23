package com.datasqrl.flinkwrapper;

import com.datasqrl.flinkwrapper.parser.SqlScriptStatementSplitter;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;

@Value
public class FlinkSqlScriptBuilder {

  StringBuilder script = new StringBuilder();

  public void append(String sql) {
    script.append(SqlScriptStatementSplitter.addStatementDelimiter(sql)).append(SqlScriptStatementSplitter.LINE_DELIMITER);
  }

  public void append(SqlNode sqlNode) {
    append(sqlNode.toString());
  }

}
