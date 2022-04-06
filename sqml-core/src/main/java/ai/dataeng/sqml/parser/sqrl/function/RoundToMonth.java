package ai.dataeng.sqml.parser.sqrl.function;

import ai.dataeng.sqml.tree.FunctionCall;

public class RoundToMonth implements RewritingFunction {

  @Override
  public boolean isAggregate() {
    return false;
  }

  @Override
  public FunctionCall rewrite(FunctionCall node) {
    return null;
  }
}
