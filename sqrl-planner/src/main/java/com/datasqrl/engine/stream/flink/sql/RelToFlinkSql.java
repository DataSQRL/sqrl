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

import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.convert.SqlNodeToString.SqlStrings;
import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;

public class RelToFlinkSql {
  public static final UnaryOperator<SqlWriterConfig> transform =
      c ->
          c.withAlwaysUseParentheses(false)
              .withSelectListItemsOnSeparateLines(false)
              .withUpdateSetListNewline(false)
              .withIndentation(1)
              .withDialect(FlinkDialect.DEFAULT)
              .withSelectFolding(null);

  public static SqlStrings convertToString(SqlNodes sqlNode) {
    return () -> sqlNode.getSqlNode().toSqlString(transform).getSql();
  }

  public static String convertToString(SqlNode sqlNode) {
    return sqlNode.toSqlString(transform).getSql();
  }

  public static List<String> convertToSqlString(List<? extends SqlNode> sqlNode) {
    return sqlNode.stream().map(RelToFlinkSql::convertToString).toList();
  }

  public static SqlNode convertToSqlNode(RelNode relNode) {
    var converter = new RelToSqlConverter(FlinkDialect.DEFAULT);
    return converter.visitRoot(relNode).asStatement();
  }
}
