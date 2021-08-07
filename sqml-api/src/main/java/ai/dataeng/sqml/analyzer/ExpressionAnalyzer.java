package ai.dataeng.sqml.analyzer;

import static java.lang.String.format;

import ai.dataeng.sqml.OperatorType;
import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.function.TypeSignature;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.BooleanLiteral;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.DecimalLiteral;
import ai.dataeng.sqml.tree.DereferenceExpression;
import ai.dataeng.sqml.tree.DoubleLiteral;
import ai.dataeng.sqml.tree.EnumLiteral;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GenericLiteral;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.InlineJoin;
import ai.dataeng.sqml.tree.IntervalLiteral;
import ai.dataeng.sqml.tree.InlineJoinExpression;
import ai.dataeng.sqml.tree.IsEmpty;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NullLiteral;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.StringLiteral;
import ai.dataeng.sqml.tree.SubqueryExpression;
import ai.dataeng.sqml.tree.TimestampLiteral;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.DateTimeSqmlType;
import ai.dataeng.sqml.type.SqmlType.NullSqmlType;
import ai.dataeng.sqml.type.SqmlType.NumberSqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.type.SqmlType.UnknownSqmlType;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import java.util.logging.Logger;

public class ExpressionAnalyzer {
  private Logger log = Logger.getLogger(Expression.class.getName());
  private final Metadata metadata;

  public ExpressionAnalyzer(Metadata metadata) {
    this.metadata = metadata;
  }

  public ExpressionAnalysis analyze(Expression node, Scope scope) {
    ExpressionAnalysis analysis = new ExpressionAnalysis();
    Visitor visitor = new Visitor(analysis);
    node.accept(visitor, new Context(scope));
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

  class Visitor extends AstVisitor<SqmlType, Context> {
    private final ExpressionAnalysis analysis;

    public Visitor(ExpressionAnalysis analysis) {
      this.analysis = analysis;
    }

    @Override
    protected SqmlType visitNode(Node node, Context context) {
      throw new RuntimeException(String.format("Could not visit node: %s %s",
          node.getClass().getName(), node));
    }

    @Override
    protected SqmlType visitIdentifier(Identifier node, Context context) {
      Optional<Field> field = context.getScope().resolveField(QualifiedName.of(node));
      if (field.isEmpty()) {
        log.warning(String.format("Could not resolve field %s", node));
        return addType(node, new UnknownSqmlType());
//        Optional<Field> d = context.getScope().resolveField(QualifiedName.of(node));
//        throw new RuntimeException(String.format("Could not resolve field %s", node.getValue()));
      }
      return addType(node, field.get().getType());
    }

    @Override
    protected SqmlType visitExpression(Expression node, Context context) {
      throw new RuntimeException(String.format("Expression needs type inference: %s. %s", 
          node.getClass().getName(), node));
    }

    @Override
    protected SqmlType visitLogicalBinaryExpression(LogicalBinaryExpression node, Context context) {
      return addType(node, new BooleanSqmlType());
    }

    @Override
    protected SqmlType visitSubqueryExpression(SubqueryExpression node, Context context) {
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(metadata, new Analysis(null));
      Scope scope = statementAnalyzer.analyze(node.getQuery(), context.getScope());

      return scope.getRelationType();
    }

    @Override
    protected SqmlType visitComparisonExpression(ComparisonExpression node, Context context) {
      return addType(node, new BooleanSqmlType());
    }

    @Override
    public SqmlType visitInlineJoinExpression(InlineJoinExpression node, Context context) {
      InlineJoin join = node.getJoin();
      RelationSqmlType rel = context.getScope().getRelation(join.getTable())
          .orElseThrow(()-> new RuntimeException(String.format("Could not find relation %s %s", join.getTable(), node)));

      if (join.getInverse().isPresent()) {
        RelationSqmlType relationSqmlType = context.getScope().getRelationType();
        rel.addField(Field.newUnqualified(join.getInverse().get().toString(), relationSqmlType));
      }

      addRelation(node.getJoin(), rel);
      return addType(node, rel);
    }

    @Override
    protected SqmlType visitArithmeticBinary(ArithmeticBinaryExpression node, Context context) {
      return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()), node.getLeft(), node.getRight());
    }

    @Override
    protected SqmlType visitFunctionCall(FunctionCall node, Context context) {
      Optional<SqmlFunction> function = metadata.getFunctionProvider().resolve(node.getName());
      if (function.isEmpty()) {
        throw new RuntimeException(String.format("Could not find function %s", node.getName()));
      }
      TypeSignature typeSignature = function.get().getTypeSignature();
      for (Expression expression : node.getArguments()) {
        expression.accept(this, context);
      }

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
      return addType(node, new StringSqmlType());
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

    @Override
    protected SqmlType visitDereferenceExpression(DereferenceExpression node, Context context) {
      SqmlType type = node.getBase().accept(this, context);
      if (!(type instanceof RelationSqmlType)) {
        throw new RuntimeException(String.format("Dereference type not a relation: %s", node));
      }

      Optional<Field> field = ((RelationSqmlType)type).resolveField(QualifiedName.of(node.getField()),
          (RelationSqmlType)type);
      if (field.isEmpty()) {
        throw new RuntimeException(String.format("Could not dereference %s in %s", node.getBase(), node.getField()));
      }

      return addType(node, field.get().getType());
    }

    @Override
    public SqmlType visitIsEmpty(IsEmpty node, Context context) {
      //tbd
      return addType(node, new StringSqmlType());
    }

    private SqmlType getOperator(Context context, Expression node, OperatorType operatorType, Expression... arguments)
    {
      //todo: Resolve operators
//      ImmutableList.Builder<SqmlType> argumentTypes = ImmutableList.builder();
//      for (Expression expression : arguments) {
//        argumentTypes.add(expression.accept(this, context));
//      }
//
//      FunctionMetadata operatorMetadata;
//      try {
//        operatorMetadata = functionAndTypeManager.getFunctionMetadata(functionAndTypeManager.resolveOperator(operatorType, fromTypes(argumentTypes.build())));
//      }
//      catch (OperatorNotFoundException e) {
//        throw new SemanticException(TYPE_MISMATCH, node, "%s", e.getMessage());
//      }
//      catch (PrestoException e) {
//        if (e.getErrorCode().getCode() == StandardErrorCode.AMBIGUOUS_FUNCTION_CALL.toErrorCode().getCode()) {
//          throw new SemanticException(SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL, node, e.getMessage());
//        }
//        throw e;
//      }
//
//      for (int i = 0; i < arguments.length; i++) {
//        Expression expression = arguments[i];
//        Type type = functionAndTypeManager.getType(operatorMetadata.getArgumentTypes().get(i));
//        coerceType(context, expression, type, format("Operator %s argument %d", operatorMetadata, i));
//      }
//
//      Type type = functionAndTypeManager.getType(operatorMetadata.getReturnType());
//      return setExpressionType(node, type);
      return addType(node, new BooleanSqmlType());
    }

    private SqmlType addType(Expression node, SqmlType type) {
      analysis.addType(node, type);
      return type;
    }

    private void addRelation(Relation relation, RelationSqmlType type) {
      analysis.setRelation(relation, type);
    }
  }
}
