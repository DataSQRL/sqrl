package ai.datasqrl.plan.calcite;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import java.util.Optional;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlParserPosFactory {

  public static SqlParserPos from(Node node) {
    return SqlParserPos.ZERO;
  }
  public SqlParserPos getPos(Optional<NodeLocation> pos) {
    return SqlParserPos.ZERO;
  }
}
