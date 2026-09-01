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
package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.WatermarkIgnoringRelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlStatementSet;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RelToFlinkSql {

  private static final Function<Integer, UnaryOperator<SqlWriterConfig>> TRANSFORM_FN =
      indentation ->
          c ->
              c.withAlwaysUseParentheses(false)
                  .withSelectListItemsOnSeparateLines(false)
                  .withUpdateSetListNewline(false)
                  .withIndentation(indentation)
                  .withDialect(FlinkDialect.DEFAULT)
                  .withSelectFolding(null);

  public static List<String> convertToSqlString(List<? extends SqlNode> sqlNode) {
    return sqlNode.stream().map(RelToFlinkSql::convertToString).toList();
  }

  public static String convertToString(SqlNode sqlNode) {
    var indentation = 1;
    if (sqlNode instanceof SqlExecute execute
        && execute.getStatement() instanceof SqlStatementSet) {
      indentation = 0;
    }

    return sqlNode.toSqlString(TRANSFORM_FN.apply(indentation)).getSql();
  }

  public static SqlNode convertToSqlNode(RelNode relNode) {
    var converter = new WatermarkIgnoringRelToSqlConverter(FlinkDialect.DEFAULT);
    return converter.visitRoot(relNode).asStatement();
  }
}
