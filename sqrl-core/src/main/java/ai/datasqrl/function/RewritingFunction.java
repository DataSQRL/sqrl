package ai.datasqrl.function;

import ai.datasqrl.parse.tree.FunctionCall;

public interface RewritingFunction extends SqrlFunction {

  FunctionCall rewrite(FunctionCall node);
}
