package ai.dataeng.sqml.parser.sqrl.function;

import ai.dataeng.sqml.tree.FunctionCall;

public interface RewritingFunction extends SqrlFunction {

  FunctionCall rewrite(FunctionCall node);
}
