package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.engine.stream.flink.sql.model.QueryPipelineItem;
import java.util.function.UnaryOperator;
import org.apache.calcite.rel.RelNode;
import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
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
    String s = sqlNode.toSqlString(
            c -> transform.apply(c.withDialect(FlinkDialect.DEFAULT)))
        .getSql().replaceAll("\"", "`");
    System.out.println("FLINK: " + s);
    return s;
  }

  public static SqlNode convertToSqlNode(RelNode optimizedNode) {
    RelToSqlConverter converter = new RelToSqlConverter(FlinkDialect.DEFAULT);
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    return sqlNode;
  }

  public static String convertToSql(FlinkRelToSqlConverter converter, RelNode optimizedNode) {
    converter.isTop = true;

    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    QueryPipelineItem query = converter.getOrCreate(QueryType.ROOT, sqlNode, optimizedNode, null);
    return query.getTableName();
  }
}
