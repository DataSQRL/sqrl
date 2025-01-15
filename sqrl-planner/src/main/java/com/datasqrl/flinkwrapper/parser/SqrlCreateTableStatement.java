package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.flinkwrapper.Sqrl2FlinkSQLTranslator;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;

@Value
public class SqrlCreateTableStatement implements SqrlDdlStatement {

  ParsedObject<String> createTable;
  SqrlComments comments;

  public SqlCreateTable toSqlNode(Sqrl2FlinkSQLTranslator sqrlEnv) {
    SqlNode result = StatementParserException.handleParseErrors(sqrlEnv::parseSQL, createTable.get(),
        createTable.getFileLocation(), 0);
    Preconditions.checkArgument(result instanceof SqlCreateTable, "Not a valid CREATE TABLE statement: %s", result);
    return (SqlCreateTable) result;
  }

}
