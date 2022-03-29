package ai.dataeng.sqml.parser.sqrl.analyzer;

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

public class ExpressionAnalyzer {

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

  class Visitor extends AstVisitor<Type, Context> {
    private final ExpressionAnalysis analysis;

    public Visitor(ExpressionAnalysis analysis) {
      this.analysis = analysis;
    }

    @Override
    public Type visitNode(Node node, Context context) {
      throw new RuntimeException(String.format("Could not visit node: %s %s",
          node.getClass().getName(), node));
    }

    @Override
    public Type visitExpression(Expression node, Context context) {
      throw new RuntimeException(String.format("Unknown expression node: %s %s",
          node.getClass().getName(), node));
    }

    @Override
    public Type visitIdentifier(Identifier node, Context context) {
      //1. Lookup identifier in current scope, throw if ambiguous
      //2. Resolve to a FieldPath
      //3. Add fieldpath to scope

      return null;
    }

    //For is not null, between, etc
    // We need to check if the returning context contains a to-many and fail if it does
    // These are not to-many compatible functions
    @Override
    public Type visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
      node.getValue().accept(this, context);
      return null;
    }

    @Override
    public Type visitBetweenPredicate(BetweenPredicate node, Context context) {
      node.getValue().accept(this, context);
      node.getMin().accept(this, context);
      node.getMax().accept(this, context);
      return null;
    }

    @Override
    public Type visitLogicalBinaryExpression(LogicalBinaryExpression node, Context context) {
      node.getLeft().accept(this, context);
      node.getRight().accept(this, context);
      return null;
    }

    @Override
    public Type visitComparisonExpression(ComparisonExpression node, Context context) {
      node.getLeft().accept(this, context);
      node.getRight().accept(this, context);

      return null;
    }

    @Override
    public Type visitArithmeticBinary(ArithmeticBinaryExpression node, Context context) {
      node.getLeft().accept(this, context);
      node.getRight().accept(this, context);

      return null;
    }

    @Override
    public Type visitFunctionCall(FunctionCall node, Context context) {
      //Get operators from Sqrl operators. they need to be sql &| flink compatible

      //Check if function call exists in the AST transformer, if it does, transform it immediately
      //If function is aggregate
      //  if it has a to-many: Register to-many to expand later
      //  if it is a to-one: Register to-one
      //If function is not an aggregate:
      //  if to-many, add to errors
      //  if to-one: register to-one

      return null;
    }

    /**
     * Allowable:
     * CASE WHEN condition1 THEN result1 (WHEN condition2 THEN result2)* (ELSE result_z) END
     */
    @Override
    public Type visitSimpleCaseExpression(SimpleCaseExpression node, Context context) {
      node.getWhenClauses().forEach(e->e.accept(this, context));
      node.getDefaultValue().map(e->e.accept(this, context));

      return null;
    }

    @Override
    public Type visitNotExpression(NotExpression node, Context context) {
      node.getValue().accept(this, context);
      return null;
    }

    @Override
    public Type visitDoubleLiteral(DoubleLiteral node, Context context) {
      return null;
    }

    @Override
    public Type visitDecimalLiteral(DecimalLiteral node, Context context) {
      return null;
    }

    @Override
    public Type visitGenericLiteral(GenericLiteral node, Context context) {
      throw new RuntimeException("Generic literal not supported yet.");
    }

    @Override
    public Type visitTimestampLiteral(TimestampLiteral node, Context context) {
      return null;
    }

    /**
     * TODO: We need to rewrite some intervals to match sql syntax
     */
    @Override
    public Type visitIntervalLiteral(IntervalLiteral node, Context context) {
      node.getExpression().accept(this, context);
      return null;
    }

    @Override
    public Type visitStringLiteral(StringLiteral node, Context context) {
      return null;
    }

    @Override
    public Type visitBooleanLiteral(BooleanLiteral node, Context context) {
      return null;
    }

    @Override
    public Type visitEnumLiteral(EnumLiteral node, Context context) {
      throw new RuntimeException("Enum literal not supported yet.");
    }

    @Override
    public Type visitNullLiteral(NullLiteral node, Context context) {
      return null;
    }

    @Override
    public Type visitLongLiteral(LongLiteral node, Context context) {
      return null;
    }
  }
}
