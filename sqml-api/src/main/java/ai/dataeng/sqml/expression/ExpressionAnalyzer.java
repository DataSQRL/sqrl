package ai.dataeng.sqml.expression;

import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.function.TypeSignature;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.BooleanLiteral;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.DecimalLiteral;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.DoubleLiteral;
import ai.dataeng.sqml.tree.EnumLiteral;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GenericLiteral;
import ai.dataeng.sqml.tree.IntervalLiteral;
import ai.dataeng.sqml.tree.JoinSubexpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NullLiteral;
import ai.dataeng.sqml.tree.StringLiteral;
import ai.dataeng.sqml.tree.TimestampLiteral;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.DateTimeSqmlType;
import ai.dataeng.sqml.type.SqmlType.NullSqmlType;
import ai.dataeng.sqml.type.SqmlType.NumberSqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.type.SqmlType.UnknownSqmlType;
import java.util.Optional;

public class ExpressionAnalyzer {
  private final Metadata metadata;

  public ExpressionAnalyzer(Metadata metadata) {
    this.metadata = metadata;
  }

  public ExpressionAnalysis analyze(Expression node, Scope scope) {
    ExpressionAnalysis analysis = new ExpressionAnalysis();
    TypeVisitor typeVisitor = new TypeVisitor(analysis);
    node.accept(typeVisitor, new Context(scope));
    return analysis;
  }

  private static class Context {
    private final Scope scope;

    public Context(Scope scope) {
      this.scope = scope;
    }

    public Scope getScope() {
      return scope;
    }
  }

  class TypeVisitor extends AstVisitor<SqmlType, Context> {
    private final ExpressionAnalysis analysis;

    public TypeVisitor(ExpressionAnalysis analysis) {
      this.analysis = analysis;
    }

    @Override
    protected SqmlType visitExpression(Expression node, Context context) {
      throw new RuntimeException(String.format("Expression needs type inference: %s. %s", 
          node.getClass().getName(), node.toString()));
    }

    @Override
    protected SqmlType visitComparisonExpression(ComparisonExpression node, Context context) {
      return addType(node, new BooleanSqmlType());
    }

    @Override
    public SqmlType visitJoinSubexpression(JoinSubexpression node, Context context) {
      return addType(node, new RelationSqmlType(node));
    }

    private SqmlType addType(Node node, SqmlType type) {
      analysis.addType(node, type);
      return type;
    }

    @Override
    protected SqmlType visitArithmeticBinary(ArithmeticBinaryExpression node, Context context) {
      return addType(node, new NumberSqmlType());
    }

    @Override
    protected SqmlType visitFunctionCall(FunctionCall node, Context context) {
      Optional<SqmlFunction> function = metadata.getFunctionProvider().resolve(node.getName());
      if (function.isEmpty()) {
        throw new RuntimeException(String.format("Could not find function %s", node.getName()));
      }
      TypeSignature typeSignature = function.get().getTypeSignature();

      return addType(node, typeSignature.getType());
    }

    @Override
    protected SqmlType visitDoubleLiteral(DoubleLiteral node, Context context) {
      return addType(node, new NumberSqmlType());
    }

    @Override
    protected SqmlType visitDecimalLiteral(DecimalLiteral node, Context context) {
      return addType(node, new NumberSqmlType());
    }

    @Override
    protected SqmlType visitGenericLiteral(GenericLiteral node, Context context) {
      return addType(node, new UnknownSqmlType());
    }

    @Override
    protected SqmlType visitTimestampLiteral(TimestampLiteral node, Context context) {
      return addType(node, new DateTimeSqmlType());
    }

    @Override
    protected SqmlType visitIntervalLiteral(IntervalLiteral node, Context context) {
      return addType(node, new DateTimeSqmlType());
    }

    @Override
    protected SqmlType visitStringLiteral(StringLiteral node, Context context) {
      return new StringSqmlType();
    }

    @Override
    protected SqmlType visitBooleanLiteral(BooleanLiteral node, Context context) {
      return addType(node, new BooleanSqmlType());
    }

    @Override
    protected SqmlType visitEnumLiteral(EnumLiteral node, Context context) {
      return addType(node, new UnknownSqmlType());
    }

    @Override
    protected SqmlType visitNullLiteral(NullLiteral node, Context context) {
      return addType(node, new NullSqmlType());
    }

    @Override
    protected SqmlType visitLongLiteral(LongLiteral node, Context context) {
      return addType(node, new NumberSqmlType());
    }
  }
}
