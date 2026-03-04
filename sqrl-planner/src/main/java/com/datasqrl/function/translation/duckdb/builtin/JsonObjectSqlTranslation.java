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
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

@AutoService(SqlTranslation.class)
public class JsonObjectSqlTranslation extends DuckDbSqlTranslation {

  public JsonObjectSqlTranslation() {
    super(BuiltInFunctionDefinitions.JSON_OBJECT);
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var jsonNullClause = (SqlLiteral) call.operand(0);
    if (SqlJsonConstructorNullClause.ABSENT_ON_NULL.name().equals(jsonNullClause.toValue())) {
      throw new UnsupportedOperationException(
          "Absent on null is not supported in DuckDB json_object()");
    }

    var jsonElements = List.copyOf(call.getOperandList().subList(1, call.getOperandList().size()));
    CalciteFunctionUtil.writeFunction("json_object", writer, jsonElements);
  }
}
