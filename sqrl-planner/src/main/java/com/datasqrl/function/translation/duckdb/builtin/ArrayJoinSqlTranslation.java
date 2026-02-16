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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

@AutoService(SqlTranslation.class)
public class ArrayJoinSqlTranslation extends DuckDbSqlTranslation {

  public ArrayJoinSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("ARRAY_JOIN"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    if (call.operandCount() == 2) {
      CalciteFunctionUtil.writeFunction("ARRAY_TO_STRING", writer, call);
      return;
    }

    var arr = call.operand(0);
    var sep = call.operand(1);
    var nullRepl = call.operand(2);

    // array_to_string(list_transform(arr, x -> coalesce(x, null_repl)), sep)
    var arrayToString = writer.startFunCall("ARRAY_TO_STRING");

    var listTransform = writer.startFunCall("LIST_TRANSFORM");
    arr.unparse(writer, 0, 0);
    writer.sep(",", true);

    writer.print("x -> ");
    var coalesce = writer.startFunCall("COALESCE");
    writer.print("x");
    writer.sep(",", true);
    nullRepl.unparse(writer, 0, 0);
    writer.endFunCall(coalesce);

    writer.endFunCall(listTransform);

    writer.sep(",", true);
    sep.unparse(writer, 0, 0);
    writer.endFunCall(arrayToString);
  }
}
