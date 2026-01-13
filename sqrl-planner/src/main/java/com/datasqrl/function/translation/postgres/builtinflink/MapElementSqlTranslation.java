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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

@AutoService(SqlTranslation.class)
public class MapElementSqlTranslation extends PostgresSqlTranslation {

  public MapElementSqlTranslation() {
    super(SqlStdOperatorTable.ITEM);
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    if (call.operandCount() == 2) {
      var baseOperand = call.operand(0);
      var indexOrKey = call.operand(1);

      var isMapBase =
          baseOperand instanceof SqlIdentifier
              || baseOperand.getKind() == SqlKind.MAP_VALUE_CONSTRUCTOR;

      // Heuristic: If the index/key is a string literal, treat as map access
      // Maps use string keys: map['name'], arrays use numeric indices: array[1]
      var isLikelyMapAccess =
          indexOrKey instanceof SqlLiteral literal && literal.getTypeName() == SqlTypeName.CHAR;

      if (isMapBase && isLikelyMapAccess) {
        // Translate map['key'] to map->>'key' for PostgreSQL JSONB access
        call.operand(0).unparse(writer, 0, 0);
        writer.print("->>");
        call.operand(1).unparse(writer, 0, 0);
        return;
      }
    }

    call.getOperator().unparse(writer, call, leftPrec, rightPrec);
  }
}
