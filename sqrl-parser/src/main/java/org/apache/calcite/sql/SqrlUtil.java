package org.apache.calcite.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqrlUtil {

  public static List<SqlParserPos> getComponentPositions(SqlIdentifier identifier) {
    return identifier.componentPositions == null ? List.of() :
        new ArrayList<>(identifier.componentPositions);
  }
}
