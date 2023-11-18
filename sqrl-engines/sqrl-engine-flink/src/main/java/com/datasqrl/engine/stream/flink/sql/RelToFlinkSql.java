package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.calcite.convert.SqlConverter.SqlNodes;
import com.datasqrl.calcite.convert.SqlToString.SqlStrings;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
import com.datasqrl.engine.stream.flink.sql.model.QueryPipelineItem;
import java.util.function.UnaryOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter.QueryType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;

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
    RelToSqlConverter converter = new RelToSqlConverter(FlinkDialect.DEFAULT);
    return converter.visitRoot(relNode).asStatement();
  }

  public static String convertToTable(FlinkRelToSqlConverter converter, RelNode relNode) {
    SqlNode sqlNode = convertToSqlNode(converter, relNode);
    QueryPipelineItem query = converter.getOrCreate(QueryType.ROOT, sqlNode, relNode, null);

    return query.getTableName();
  }

  public static SqlNode convertToSqlNode(FlinkRelToSqlConverter converter, RelNode relNode) {
    return converter.visitRoot(relNode).asStatement();
  }

}
