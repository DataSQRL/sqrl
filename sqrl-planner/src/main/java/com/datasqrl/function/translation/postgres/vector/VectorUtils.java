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
package com.datasqrl.function.translation.postgres.vector;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

final class VectorUtils {

  private static final SqlSpecialOperator CAST_TO_VECTOR =
      new SqlSpecialOperator("CAST_TO_VECTOR", SqlKind.OTHER) {
        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
          call.operand(0).unparse(writer, leftPrec, rightPrec);
          writer.print("::vector");
        }
      };

  static List<SqlNode> castDynamicParamsToVector(List<SqlNode> nodes) {
    return nodes.stream()
        .map(
            sqlNode ->
                sqlNode instanceof SqlDynamicParam
                    ? CAST_TO_VECTOR.createCall(SqlParserPos.ZERO, sqlNode)
                    : sqlNode)
        .toList();
  }
}
