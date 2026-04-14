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
package com.datasqrl.function.translation.duckdb.builtin;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.translation.DuckDbSqlTranslation;
import com.datasqrl.function.translation.SqlTranslation;
import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class RegexpInstrSqlTranslation extends DuckDbSqlTranslation {

  public RegexpInstrSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("REGEXP_INSTR"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    // Translate REGEXP_INSTR(str, pattern) ->
    //   CASE WHEN regexp_matches(str, pattern)
    //        THEN strpos(str, regexp_extract(str, pattern))
    //        ELSE 0 END
    var str = call.operand(0);
    var pattern = call.operand(1);

    var condition =
        CalciteFunctionUtil.lightweightOp("regexp_matches")
            .createCall(SqlParserPos.ZERO, str, pattern);

    var extract =
        CalciteFunctionUtil.lightweightOp("regexp_extract")
            .createCall(SqlParserPos.ZERO, str, pattern);

    var strpos =
        CalciteFunctionUtil.lightweightOp("strpos").createCall(SqlParserPos.ZERO, str, extract);

    var zero = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);

    SqlStdOperatorTable.CASE
        .createCall(
            SqlParserPos.ZERO,
            null,
            new SqlNodeList(List.of(condition), SqlParserPos.ZERO),
            new SqlNodeList(List.of(strpos), SqlParserPos.ZERO),
            zero)
        .unparse(writer, leftPrec, rightPrec);
  }
}
