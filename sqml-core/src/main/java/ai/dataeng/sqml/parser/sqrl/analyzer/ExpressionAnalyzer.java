package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.sqrl.PathUtil;
import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.parser.sqrl.function.RewritingFunction;
import ai.dataeng.sqml.parser.sqrl.function.SqrlFunction;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.BetweenPredicate;
import ai.dataeng.sqml.tree.BooleanLiteral;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.DecimalLiteral;
import ai.dataeng.sqml.tree.DoubleLiteral;
import ai.dataeng.sqml.tree.EnumLiteral;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GenericLiteral;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.IntervalLiteral;
import ai.dataeng.sqml.tree.IsNotNullPredicate;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NotExpression;
import ai.dataeng.sqml.tree.NullLiteral;
import ai.dataeng.sqml.tree.SimpleCaseExpression;
import ai.dataeng.sqml.tree.StringLiteral;
import ai.dataeng.sqml.tree.TimestampLiteral;
import ai.dataeng.sqml.type.Type;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExpressionAnalyzer {
  FunctionLookup functionLookup = new FunctionLookup();
  private Map<FunctionCall, RewritingFunction> rewriteFunction = new HashMap<>();
  private List<FunctionCall> toMany = new ArrayList<>();
  private List<FunctionCall> toOne = new ArrayList<>();
  public ExpressionAnalyzer() {
  }

  public static class ExpressionAnalysis {

  }

  public ExpressionAnalysis analyze(Expression node, Scope scope) {
    ExpressionAnalysis analysis = new ExpressionAnalysis();
    Visitor visitor = new Visitor(analysis);
    node.accept(visitor, new Context(scope));
    return analysis;
  }

  public static class Context {
    private final Scope scope;

    public Context(Scope scope) {
      this.scope = scope;
    }

    public Scope getScope() {
      return scope;
    }
  }

  class Visitor extends AstVisitor<Void, Context> {
    private final ExpressionAnalysis analysis;

    public Visitor(ExpressionAnalysis analysis) {
      this.analysis = analysis;
    }

    @Override
    public Void visitNode(Node node, Context context) {
      throw new RuntimeException(String.format("Could not visit node: %s %s",
          node.getClass().getName(), node));
    }

    @Override
    public Void visitExpression(Expression node, Context context) {
      throw new RuntimeException(String.format("Unknown expression node: %s %s",
          node.getClass().getName(), node));
    }

    @Override
    public Void visitIdentifier(Identifier node, Context context) {
      List<FieldPath> fieldPaths = context.getScope()
          .resolve(node.getNamePath());

      Preconditions.checkState(fieldPaths.size() == 1,
        "Could not resolve field (ambiguous or non-existent: " + node + " : " + fieldPaths);

      node.setResolved(fieldPaths.get(0));

      return null;
    }

    //For is not null, between, etc
    // We need to check if the returning context contains a to-many and fail if it does
    // These are not to-many compatible functions
    @Override
    public Void visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
      node.getValue().accept(this, context);
      return null;
    }

    @Override
    public Void visitBetweenPredicate(BetweenPredicate node, Context context) {
      node.getValue().accept(this, context);
      node.getMin().accept(this, context);
      node.getMax().accept(this, context);
      return null;
    }

    @Override
    public Void visitLogicalBinaryExpression(LogicalBinaryExpression node, Context context) {
      node.getLeft().accept(this, context);
      node.getRight().accept(this, context);
      return null;
    }

    @Override
    public Void visitComparisonExpression(ComparisonExpression node, Context context) {
      node.getLeft().accept(this, context);
      node.getRight().accept(this, context);

      return null;
    }

    @Override
    public Void visitArithmeticBinary(ArithmeticBinaryExpression node, Context context) {
      node.getLeft().accept(this, context);
      node.getRight().accept(this, context);

      return null;
    }

    @Override
    public Void visitFunctionCall(FunctionCall node, Context context) {
      SqrlFunction function = functionLookup.lookup(node.getName());
      if (function instanceof RewritingFunction) {
        rewriteFunction.put(node, (RewritingFunction) function);
      }

      for (Expression arg : node.getArguments()) {
        arg.accept(this, context);
      }

      //Todo: allow expanding aggregates more than a single argument
      if (function.isAggregate() && node.getArguments().size() == 1 &&
          node.getArguments().get(0) instanceof Identifier) {
        Identifier identifier = (Identifier)node.getArguments().get(0);
        FieldPath fieldPath = identifier.getResolved();
        if (PathUtil.isToMany(fieldPath)) {
          toMany.add(node);
        } else if (PathUtil.isToOne(fieldPath)) {
          toOne.add(node);
        }
      }

      return null;
    }

    /**
     * Allowable:
     * CASE WHEN condition1 THEN result1 (WHEN condition2 THEN result2)* (ELSE result_z) END
     */
    @Override
    public Void visitSimpleCaseExpression(SimpleCaseExpression node, Context context) {
      node.getWhenClauses().forEach(e->e.accept(this, context));
      node.getDefaultValue().map(e->e.accept(this, context));

      return null;
    }

    @Override
    public Void visitNotExpression(NotExpression node, Context context) {
      node.getValue().accept(this, context);
      return null;
    }

    @Override
    public Void visitDoubleLiteral(DoubleLiteral node, Context context) {
      return null;
    }

    @Override
    public Void visitDecimalLiteral(DecimalLiteral node, Context context) {
      return null;
    }

    @Override
    public Void visitGenericLiteral(GenericLiteral node, Context context) {
      throw new RuntimeException("Generic literal not supported yet.");
    }

    @Override
    public Void visitTimestampLiteral(TimestampLiteral node, Context context) {
      return null;
    }

    /**
     * TODO: We need to rewrite some intervals to match sql syntax
     */
    @Override
    public Void visitIntervalLiteral(IntervalLiteral node, Context context) {
      node.getExpression().accept(this, context);
      return null;
    }

    @Override
    public Void visitStringLiteral(StringLiteral node, Context context) {
      return null;
    }

    @Override
    public Void visitBooleanLiteral(BooleanLiteral node, Context context) {
      return null;
    }

    @Override
    public Void visitEnumLiteral(EnumLiteral node, Context context) {
      throw new RuntimeException("Enum literal not supported yet.");
    }

    @Override
    public Void visitNullLiteral(NullLiteral node, Context context) {
      return null;
    }

    @Override
    public Void visitLongLiteral(LongLiteral node, Context context) {
      return null;
    }
  }
}
