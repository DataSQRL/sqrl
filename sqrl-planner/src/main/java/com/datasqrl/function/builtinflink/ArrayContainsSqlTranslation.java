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
package com.datasqrl.function.builtinflink;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.PgSpecificOperatorTable;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

@AutoService(SqlTranslation.class)
public class ArrayContainsSqlTranslation extends PostgresSqlTranslation {

  public ArrayContainsSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp(BuiltInFunctionDefinitions.ARRAY_CONTAINS));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var array = call.getOperandList().get(0);
    var value = call.getOperandList().get(1);

    // Emit: value = ANY(array)
    PgSpecificOperatorTable.EqualsAny.createCall(SqlParserPos.ZERO, value, array)
        .unparse(writer, leftPrec, rightPrec);
  }
}
