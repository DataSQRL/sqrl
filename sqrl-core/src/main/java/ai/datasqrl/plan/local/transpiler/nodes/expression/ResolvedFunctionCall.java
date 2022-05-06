package ai.datasqrl.plan.local.transpiler.nodes.expression;

import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.FunctionCall;
import lombok.Getter;

@Getter
public class ResolvedFunctionCall extends FunctionCall {

  private final FunctionCall oldExpression;
  private final SqrlFunction function;

  public ResolvedFunctionCall(FunctionCall functionCall, SqrlFunction function) {
    super(functionCall.getLocation(), functionCall.getNamePath(),
        functionCall.getArguments(), functionCall.isDistinct(),
        functionCall.getOver());
    this.oldExpression = functionCall;
    this.function = function;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitResolvedFunctionCall(this, context);
  }
}
