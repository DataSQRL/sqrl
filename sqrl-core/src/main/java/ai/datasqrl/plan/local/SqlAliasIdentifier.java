package ai.datasqrl.plan.local;

import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAliasIdentifier extends SqlIdentifier {
  public SqlAliasIdentifier(List<String> names, SqlParserPos pos) {
    super(names, pos);
  }
}