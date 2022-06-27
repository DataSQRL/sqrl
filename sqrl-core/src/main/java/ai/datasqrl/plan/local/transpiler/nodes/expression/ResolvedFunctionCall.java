package ai.datasqrl.plan.local.transpiler.nodes.expression;

import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.Window;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = false)
public class ResolvedFunctionCall extends Expression {

  private final NamePath namePath;
  private final List<Expression> arguments;
  private final boolean distinct;
  private final Optional<Window> over;

  private final FunctionCall oldExpression;
  private final SqrlAwareFunction function;

  public ResolvedFunctionCall(Optional<NodeLocation> location, NamePath namePath,
      List<Expression> arguments, boolean distinct, Optional<Window> over,
      FunctionCall oldExpression,
      SqrlAwareFunction function) {
    super(location);
    this.namePath = namePath;
    this.arguments = arguments;
    this.distinct = distinct;
    this.over = over;

    this.oldExpression = oldExpression;
    this.function = function;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitResolvedFunctionCall(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }
}
