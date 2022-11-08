package ai.datasqrl.plan.calcite.util;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter.SubQueryStyle;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

public class SqlNodePrinter {

  public static String printJoin(SqlNode from) {
    SqlPrettyWriter sqlWriter = new SqlPrettyWriter();
    sqlWriter.startList("", "");
    from.unparse(sqlWriter, 0, 0);
    return sqlWriter.toString();
  }

  public static String toString(SqlNode node) {
    SqlWriterConfig config = RelToSql.transform.apply(SqlPrettyWriter.config());
    SqlPrettyWriter writer = new SqlPrettyWriter(config);

    node.unparse(writer, 0, 0);
    return writer.toSqlString().getSql();
  }
}
