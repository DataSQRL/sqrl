package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.tree.NodeLocation;
import java.util.Optional;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlParserPosFactory {

  public SqlParserPos getPos(NodeLocation pos) {
    return SqlParserPos.ZERO;
  }
  public SqlParserPos getPos(Optional<NodeLocation> pos) {
    return SqlParserPos.ZERO;
  }
}
