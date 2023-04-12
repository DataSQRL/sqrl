package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.engine.stream.flink.sql.plan.QueryPipelineItem;
import java.util.function.UnaryOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.FlinkDialect;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter.QueryType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class RelToFlinkSql {
  public static final UnaryOperator<SqlWriterConfig> transform = c ->
      c.withAlwaysUseParentheses(false)
          .withSelectListItemsOnSeparateLines(false)
          .withUpdateSetListNewline(false)
          .withIndentation(1)
          .withQuoteAllIdentifiers(true)
          .withDialect(PostgresqlSqlDialect.DEFAULT)
          .withSelectFolding(null);

  public static String convertToString(RelNode optimizedNode) {
    return convertToSqlNode(optimizedNode).toSqlString(
            c -> transform.apply(c.withDialect(FlinkDialect.DEFAULT)))
        .getSql();
  }

  public static String convertToString(SqlNode sqlNode) {
    return sqlNode.toSqlString(
            c -> transform.apply(c.withDialect(FlinkDialect.DEFAULT)))
        .getSql()
        .replaceAll("\"", "`");
  }

  public static SqlNode convertToSqlNode(RelNode optimizedNode) {
    RelToSqlConverter converter = new RelToSqlConverter(FlinkDialect.DEFAULT);
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    return sqlNode;
  }

  public static String convertToSql(FlinkRelToSqlConverter converter, RelNode optimizedNode) {
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    QueryPipelineItem query = converter.getOrCreate(QueryType.ROOT, sqlNode, optimizedNode, null);
    return query.getTableName();
  }
}
