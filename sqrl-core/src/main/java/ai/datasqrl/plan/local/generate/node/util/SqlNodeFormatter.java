package ai.datasqrl.plan.local.generate.node.util;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter.SubQueryStyle;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

public class SqlNodeFormatter {

  public static String toString(SqlNode node) {
    SqlWriterConfig config = SqlPrettyWriter.config().withAlwaysUseParentheses(false)
        .withIndentation(2)
        .withSubQueryStyle(SubQueryStyle.HYDE)
        .withSelectListItemsOnSeparateLines(true)
        ;
    SqlPrettyWriter writer = new SqlPrettyWriter(config);

    node.unparse(writer, 0, 0);
    return writer.toSqlString().getSql();
  }
}
