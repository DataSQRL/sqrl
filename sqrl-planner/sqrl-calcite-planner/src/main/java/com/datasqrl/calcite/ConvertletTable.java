package com.datasqrl.calcite;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public class ConvertletTable implements SqlRexConvertletTable {
  @Override
  public SqlRexConvertlet get(SqlCall call) {
    return StandardConvertletTable.INSTANCE.get(call);
  }
}
