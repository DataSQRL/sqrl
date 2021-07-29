package ai.dataeng.sqml.expression;

import ai.dataeng.sqml.ResolvedField;
import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.function.TypeSignature;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression.Operator;
import ai.dataeng.sqml.tree.BinaryLiteral;
import ai.dataeng.sqml.tree.BooleanLiteral;
import ai.dataeng.sqml.tree.CharLiteral;
import ai.dataeng.sqml.tree.DecimalLiteral;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.DoubleLiteral;
import ai.dataeng.sqml.tree.EnumLiteral;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GenericLiteral;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.IntervalLiteral;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.NullLiteral;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.StringLiteral;
import ai.dataeng.sqml.tree.TimestampLiteral;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.Type.BooleanType;
import ai.dataeng.sqml.type.Type.DateType;
import ai.dataeng.sqml.type.Type.NullType;
import ai.dataeng.sqml.type.Type.NumberType;
import ai.dataeng.sqml.type.Type.StringType;
import ai.dataeng.sqml.type.Type.UnknownType;

public class ExpressionAnalyzer {
  private final Metadata metadata;

  public ExpressionAnalyzer(Metadata metadata) {
    this.metadata = metadata;
  }

  public ExpressionAnalysis analyze(Expression node, Scope scope) {
    TypeVisitor typeVisitor = new TypeVisitor();
    node.accept(typeVisitor, new Context(scope));
    return null;
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

  class TypeVisitor extends DefaultTraversalVisitor<Type, Context> {

    @Override
    protected Type visitExpression(Expression node, Context context) {
      throw new RuntimeException(String.format("Expression needs type inference: %s", node.getClass().getName()));
    }

    @Override
    protected Type visitIdentifier(Identifier node, Context context) {
      ResolvedField field = context.getScope().resolveField(node, QualifiedName.of(node.getValue()));

      return null;//field.getType();
    }

    @Override
    protected Type visitArithmeticBinary(ArithmeticBinaryExpression node, Context context) {
      Type leftType = node.getLeft().accept(this, context);
      Type rightType = node.getRight().accept(this, context);
      if (leftType instanceof StringType || rightType instanceof StringType) {
        return new StringType();
      }

      return new NumberType();
    }

    @Override
    protected Type visitFunctionCall(FunctionCall node, Context context) {
      SqmlFunction function = metadata.getFunctionProvider().resolve(node.getName());
      TypeSignature typeSignature = function.getTypeSignature();

      return typeSignature.getType();
    }

    @Override
    protected Type visitDoubleLiteral(DoubleLiteral node, Context context) {
      return new NumberType();
    }

    @Override
    protected Type visitDecimalLiteral(DecimalLiteral node, Context context) {
      return new NumberType();
    }

    @Override
    protected Type visitGenericLiteral(GenericLiteral node, Context context) {
      return new UnknownType();
    }

    @Override
    protected Type visitTimestampLiteral(TimestampLiteral node, Context context) {
      return new DateType();
    }

    @Override
    protected Type visitIntervalLiteral(IntervalLiteral node, Context context) {
      return new DateType();
    }

    @Override
    protected Type visitStringLiteral(StringLiteral node, Context context) {
      return new StringType();
    }

    @Override
    protected Type visitCharLiteral(CharLiteral node, Context context) {
      return new StringType();
    }

    @Override
    protected Type visitBinaryLiteral(BinaryLiteral node, Context context) {
      return new UnknownType();
    }

    @Override
    protected Type visitBooleanLiteral(BooleanLiteral node, Context context) {
      return new BooleanType();
    }

    @Override
    protected Type visitEnumLiteral(EnumLiteral node, Context context) {
      return new UnknownType();
    }

    @Override
    protected Type visitNullLiteral(NullLiteral node, Context context) {
      return new NullType();
    }

    @Override
    protected Type visitLongLiteral(LongLiteral node, Context context) {
      return new NumberType();
    }
  }
}
