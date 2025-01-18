package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.flinkwrapper.Sqrl2FlinkSQLTranslator;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;

@Value
public class SqrlCreateTableStatement implements SqrlDdlStatement {

  ParsedObject<String> createTable;
  SqrlComments comments;

  public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv) {
    return createTable.get();
  }

  @Override
  public FileLocation mapSqlLocation(FileLocation location) {
    return createTable.getFileLocation().add(location);
  }
}
