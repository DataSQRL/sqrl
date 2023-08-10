
package com.datasqrl.calcite.function.builtin;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.function.ITransformation;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.Instant;
import java.util.List;

public class NOW extends ScalarFunction implements ITransformation {

  public Instant eval() {
    return Instant.now();
  }

  @Override
  public SqlNode apply(String dialect, SqlOperator op, SqlParserPos pos, List<SqlNode> nodeList) {
    switch (dialect) {
      case "POSTGRES":
      default:
        return new SqlUnresolvedFunction(new SqlIdentifier("NOW2", SqlParserPos.ZERO),
          null, null, null, null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION)
            .createCall(pos, nodeList);
    }
  }
}