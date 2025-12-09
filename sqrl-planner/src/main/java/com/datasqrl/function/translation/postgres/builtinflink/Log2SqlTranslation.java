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
package com.datasqrl.function.translation.postgres.builtinflink;

import com.datasqrl.function.translation.PostgresSqlTranslation;
import com.datasqrl.function.translation.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

@AutoService(SqlTranslation.class)
public class Log2SqlTranslation extends PostgresSqlTranslation {

  public Log2SqlTranslation() {
    super(BuiltInFunctionDefinitions.LOG2);
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var base = SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO);

    var log = writer.startFunCall("LOG");
    base.unparse(writer, 0, 0);
    writer.sep(",", true);
    call.operand(0).unparse(writer, 0, 0);
    writer.endFunCall(log);
  }
}
