package ai.datasqrl.function;

import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import java.util.Optional;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class Now implements RewritingFunction {

  @Override
  public boolean isAggregate() {
    return false;
  }

  @Override
  public boolean requiresOver() {
    return false;
  }

  @Override
  public FunctionCall rewrite(FunctionCall node) {
    return new FunctionCall(node.getLocation(),
        Name.system(SqrlOperatorTable.CURRENT_TIMESTAMP.getName()).toNamePath(),
        node.getArguments(),
        false,
        Optional.empty());
  }
}
