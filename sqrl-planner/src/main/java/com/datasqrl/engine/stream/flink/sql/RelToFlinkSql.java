package com.datasqrl.engine.stream.flink.sql;

import java.util.function.UnaryOperator;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;

import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.convert.SqlNodeToString.SqlStrings;
import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;

public class RelToFlinkSql {
  public static final UnaryOperator<SqlWriterConfig> transform = c ->
      c.withAlwaysUseParentheses(false)
          .withSelectListItemsOnSeparateLines(false)
          .withUpdateSetListNewline(false)
          .withIndentation(1)
          .withDialect(FlinkDialect.DEFAULT)
          .withSelectFolding(null);

  public static SqlStrings convertToString(SqlNodes sqlNode) {
    return () -> sqlNode.getSqlNode().toSqlString(transform)
        .getSql();
  }

  public static String convertToString(SqlNode sqlNode) {
    return sqlNode.toSqlString(transform)
        .getSql();
  }

  public static SqlNode convertToSqlNode(RelNode relNode) {
    var converter = new RelToSqlConverter(FlinkDialect.DEFAULT);
    return converter.visitRoot(relNode).asStatement();
  }

}
