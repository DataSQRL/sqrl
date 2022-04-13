package ai.dataeng.sqml.parser.sqrl.function;

import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.name.Name;
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
