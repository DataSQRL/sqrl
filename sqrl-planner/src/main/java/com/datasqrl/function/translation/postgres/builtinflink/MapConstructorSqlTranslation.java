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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

@AutoService(SqlTranslation.class)
public class MapConstructorSqlTranslation extends PostgresSqlTranslation {

  public MapConstructorSqlTranslation() {
    super(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR);
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    // Use DuckDB map syntax for now: MAP { <key>: <val>, ... }
    // TODO: untie DuckDb translations from Postgres
    writer.keyword("MAP");
    var elemList = writer.startList("{", "}");
    for (int i = 0; i < call.operandCount(); i++) {
      call.operand(i).unparse(writer, 0, 0);
      if (i >= call.operandCount() - 1) {
        continue;
      }
      writer.sep(i % 2 == 1 ? "," : ":");
    }
    writer.endList(elemList);
  }
}
