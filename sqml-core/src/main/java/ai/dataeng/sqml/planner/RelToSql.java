package ai.dataeng.sqml.planner;

import java.util.function.UnaryOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class RelToSql {

  public static String convertToSql(RelNode optimizedNode) {

    RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    UnaryOperator<SqlWriterConfig> transform = c ->
        c.withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(0);

    String sql = sqlNode.toSqlString(c -> transform.apply(c.withDialect(PostgresqlSqlDialect.DEFAULT)))
        .getSql();
    return sql;
  }
}
