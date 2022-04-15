package ai.datasqrl.sql.calcite;

import ai.datasqrl.parse.tree.NodeLocation;
import java.util.Optional;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlParserPosFactory {
  public SqlParserPos getPos(Optional<NodeLocation> pos) {
    return SqlParserPos.ZERO;
  }
}
