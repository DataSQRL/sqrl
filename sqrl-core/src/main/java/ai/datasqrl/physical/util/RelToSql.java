package ai.datasqrl.physical.util;

import java.util.function.UnaryOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class RelToSql {

  public static SqlNode convertToSqlNode(RelNode optimizedNode) {
    RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    return sqlNode;
  }

  public static String convertToSql(RelNode optimizedNode) {
    UnaryOperator<SqlWriterConfig> transform = c ->
        c.withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(1)
            .withSelectFolding(null);

    String sql = convertToSqlNode(optimizedNode).toSqlString(
            c -> transform.apply(c.withDialect(PostgresqlSqlDialect.DEFAULT)))
        .getSql();
    return sql;
  }
}
