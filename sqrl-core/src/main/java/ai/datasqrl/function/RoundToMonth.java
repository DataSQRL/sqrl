package ai.datasqrl.function;

import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.name.Name;
import java.util.Optional;

public class RoundToMonth implements RewritingFunction {

  @Override
  public boolean isAggregate() {
    return false;
  }

  @Override
  public FunctionCall rewrite(FunctionCall node) {
    return new FunctionCall(node.getLocation(), Name.system("COALESCE").toNamePath(), node.getArguments(), false,
        Optional.empty());
  }
}
