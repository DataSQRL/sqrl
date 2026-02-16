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
import java.util.ArrayList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

@AutoService(SqlTranslation.class)
public class EltSqlTranslation extends DuckDbSqlTranslation {

  public EltSqlTranslation() {
    super(BuiltInFunctionDefinitions.ELT);
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var idxOperand = call.operand(0);

    // Put index operand last
    var operands = new ArrayList<SqlNode>(call.operandCount());
    for (int i = 1; i < call.operandCount(); i++) {
      operands.add(call.operand(i));
    }

    var fn = writer.startFunCall("list_extract");
    var args = writer.startList("[", "]");
    CalciteFunctionUtil.writeOperands(writer, operands);
    writer.endList(args);
    writer.sep(",", true);
    idxOperand.unparse(writer, 0, 0);
    writer.endFunCall(fn);
  }
}
