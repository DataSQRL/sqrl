package ai.dataeng.sqml.planner.macros;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

@AllArgsConstructor
public class SqlNodeMapper extends SqlShuttle {
  private final Map<SqlNode, SqlNode> mapper;

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    if (mapper.containsKey(call)) {
      return mapper.get(call);
    }
    return super.visit(call);
  }

  @Override
  public @Nullable SqlNode visit(SqlIdentifier id) {
    if (mapper.containsKey(id)) {
      return mapper.get(id);
    }
    return super.visit(id);
  }
}
