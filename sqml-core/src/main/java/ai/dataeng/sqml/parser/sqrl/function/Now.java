package ai.dataeng.sqml.parser.sqrl.function;

import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.name.Name;
import java.util.Optional;

public class Now implements RewritingFunction {

  @Override
  public boolean isAggregate() {
    return false;
  }

  @Override
  public FunctionCall rewrite(FunctionCall node) {
    return new FunctionCall(node.getLocation(), Name.system("CURRENT_DATE").toNamePath(), node.getArguments(), false,
        Optional.empty());
  }
}
