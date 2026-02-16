/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.function.translation;

import com.datasqrl.function.CalciteFunctionUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public final class PostgresLikeTranslations {

  public static void arrayPrepend(SqlCall call, SqlWriter writer) {
    var arrayPrepend = writer.startFunCall("ARRAY_PREPEND");
    call.operand(1).unparse(writer, 0, 0);
    writer.sep(",", true);
    call.operand(0).unparse(writer, 0, 0);
    writer.endFunCall(arrayPrepend);
  }

  public static void convertTz(SqlCall call, SqlWriter writer) {
    var dt = call.operand(0);
    var srcTz = call.operand(1);
    var destTz = call.operand(2);
    var paren = writer.startList("(", ")");
    writer.keyword("TIMESTAMP");
    dt.unparse(writer, 0, 0);
    writer.keyword("AT TIME ZONE");
    srcTz.unparse(writer, 0, 0);
    writer.endList(paren);
    writer.keyword("AT TIME ZONE");
    destTz.unparse(writer, 0, 0);
  }

  public static void e(SqlWriter writer, int leftPrec, int rightPrec) {
    var one = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

    SqlStdOperatorTable.EXP.createCall(SqlParserPos.ZERO, one).unparse(writer, leftPrec, rightPrec);
  }

  public static void isAlpha(SqlCall call, SqlWriter writer) {
    var str = call.operand(0);
    var pattern = SqlLiteral.createCharString("^[A-Za-z]+$", SqlParserPos.ZERO);

    var paren = writer.startList("(", ")");
    str.unparse(writer, 0, 0);
    writer.print("~ ");
    pattern.unparse(writer, 0, 0);
    writer.endFunCall(paren);
  }

  public static void isDecimal(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var string = call.operand(0);
    var pattern = SqlLiteral.createCharString("^[+-]?[0-9]+(\\.[0-9]+)?$", SqlParserPos.ZERO);

    SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE
        .createCall(SqlParserPos.ZERO, string, pattern)
        .unparse(writer, leftPrec, rightPrec);
  }

  public static void isDigit(SqlCall call, SqlWriter writer) {
    var str = call.operand(0);
    var pattern = SqlLiteral.createCharString("^[0-9]+$", SqlParserPos.ZERO);

    var paren = writer.startList("(", ")");
    str.unparse(writer, 0, 0);
    writer.print("~ ");
    pattern.unparse(writer, 0, 0);
    writer.endFunCall(paren);
  }

  public static void rand(SqlCall call, SqlWriter writer, String dbType) {
    if (call.operandCount() > 0) {
      throw new UnsupportedOperationException(
          "%s does not support RAND(seed) in a single expression. Use RAND() instead."
              .formatted(dbType));
    }

    var random = writer.startFunCall("random");
    writer.endFunCall(random);
  }

  public static void randInteger(SqlCall call, SqlWriter writer, String dbType) {
    if (call.operandCount() != 1) {
      throw new UnsupportedOperationException(
          "%s does not support RAND_INTEGER(seed, end) in a single expression. Use RAND_INTEGER(end) instead."
              .formatted(dbType));
    }

    var floor = writer.startFunCall("floor");
    var random = writer.startFunCall("random");
    writer.endFunCall(random);
    writer.sep("*", true);
    call.operand(0).unparse(writer, 0, 0);
    writer.endFunCall(floor);
  }

  public static void splitIndex(SqlCall call, SqlWriter writer) {
    var operands = new ArrayList<>(call.getOperandList());

    // Flink SPLIT_INDEX is 0-based, Postgres SPLIT_PART is 1-based.
    var idxNode = operands.get(2);

    SqlNode newIdxNode;
    if (idxNode instanceof SqlLiteral idxLiteral) {
      var idxVal = idxLiteral.intValue(false);
      newIdxNode = SqlLiteral.createExactNumeric(String.valueOf(idxVal + 1), SqlParserPos.ZERO);

    } else {
      // Index can be an expression (e.g. 1 - 1). Preserve the expression and add 1.
      var one = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
      newIdxNode =
          new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(idxNode, one), SqlParserPos.ZERO);
    }

    operands.set(2, newIdxNode);

    CalciteFunctionUtil.writeFunction("split_part", writer, operands);
  }

  public static void truncate(SqlCall call, SqlWriter writer) {
    CalciteFunctionUtil.writeFunction("trunc", writer, call);
  }

  public static void unixTimestamp(SqlCall call, SqlWriter writer, String dbType) {
    if (call.operandCount() > 0) {
      throw new UnsupportedOperationException(
          "Calling UNIX_TIMESTAMP(...) with args is not supported yet for %s.".formatted(dbType));
    }

    writer.print("(");
    var extract = writer.startFunCall("EXTRACT");
    writer.keyword("EPOCH");
    writer.keyword("FROM");
    writer.keyword("CURRENT_TIMESTAMP");
    writer.endFunCall(extract);
    writer.print("::bigint)");
  }
}
