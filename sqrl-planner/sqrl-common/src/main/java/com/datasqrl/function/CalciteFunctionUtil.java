package com.datasqrl.function;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.parser.SqlParserPos;

public class CalciteFunctionUtil {

  /**
   * Operator that is used generally for rex planning where no operand inference or return type
   * inference happens.
   */
  public static SqlUnresolvedFunction lightweightOp(String name) {
    return new SqlUnresolvedFunction(new SqlIdentifier(name, SqlParserPos.ZERO),
        null, null, null,
        null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }
}
