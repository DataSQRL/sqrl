package ai.dataeng.sqml.analyzer;

import static java.lang.String.format;

import ai.dataeng.sqml.OperatorType;
import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.function.TypeSignature;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.schema2.basic.DateTimeType;
import ai.dataeng.sqml.schema2.basic.NullType;
import ai.dataeng.sqml.schema2.basic.NumberType;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.BetweenPredicate;
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
import ai.dataeng.sqml.tree.InlineJoinBody;
import ai.dataeng.sqml.tree.IntervalLiteral;
import ai.dataeng.sqml.tree.IsEmpty;
import ai.dataeng.sqml.tree.IsNotNullPredicate;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NotExpression;
import ai.dataeng.sqml.tree.NullLiteral;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.SimpleCaseExpression;
import ai.dataeng.sqml.tree.StringLiteral;
import ai.dataeng.sqml.tree.SubqueryExpression;
import ai.dataeng.sqml.tree.TimestampLiteral;
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
    protected Type visitNode(Node node, Context context) {
      throw new RuntimeException(String.format("Could not visit node: %s %s",
          node.getClass().getName(), node));
    }

    @Override
    protected Type visitIdentifier(Identifier node, Context context) {
      Optional<Type> resolvedType = context.getScope().resolveType(QualifiedName.of(node));
      if (resolvedType.isEmpty()) {
        throw new RuntimeException(String.format("Could not resolve identifier %s", node));
      }
      return addType(node, resolvedType.get());
    }

    @Override
    protected Type visitExpression(Expression node, Context context) {
      throw new RuntimeException(String.format("Expression needs type inference: %s. %s", 
          node.getClass().getName(), node));
    }

    @Override
    protected Type visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
      node.getValue().accept(this, context);
      return addType(node, new BooleanType());
    }

    @Override
    protected Type visitBetweenPredicate(BetweenPredicate node, Context context) {
      node.getValue().accept(this, context);
      return addType(node, new BooleanType());
    }

    @Override
    protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, Context context) {
      node.getLeft().accept(this, context);
      node.getRight().accept(this, context);
      return addType(node, new BooleanType());
    }

    @Override
    protected Type visitSubqueryExpression(SubqueryExpression node, Context context) {
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(metadata, new Analysis(null));
      Scope scope = node.getQuery().accept(statementAnalyzer, context.getScope());

      return scope.getRelation();
    }

    @Override
    protected Type visitComparisonExpression(ComparisonExpression node, Context context) {
      Type left = node.getLeft().accept(this, context);
      Type right = node.getRight().accept(this, context);

      //todo determine if compariable

      return addType(node, new BooleanType());
    }

    @Override
    public Type visitInlineJoinBody(InlineJoinBody node, Context context) {
      //Todo: Walk the join
//      RelationType rel = context.getScope().resolveRelation(node.getTable())
//          .orElseThrow(()-> new RuntimeException(String.format("Could not find relation %s %s", node.getTable(), node)));

//      if (node.getInverse().isPresent()) {
//        RelationType relationType = context.getScope().getRelation();
//        rel.addField(Field.newUnqualified(node.getInverse().get().toString(), relationType));
//      }

//      addRelation(node.getJoin(), rel);
//      return addType(node, rel);
      return null;
    }

    @Override
    protected Type visitArithmeticBinary(ArithmeticBinaryExpression node, Context context) {
      return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()), node.getLeft(), node.getRight());
    }

    @Override
    protected Type visitFunctionCall(FunctionCall node, Context context) {
      //Todo: Function calls can accept a relation
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
    protected Type visitSimpleCaseExpression(SimpleCaseExpression node, Context context) {
      //todo case when
      return addType(node, new StringType());
    }

    @Override
    protected Type visitNotExpression(NotExpression node, Context context) {
      return addType(node, new BooleanType());
    }

    @Override
    protected Type visitDoubleLiteral(DoubleLiteral node, Context context) {
      return addType(node, new NumberType());
    }

    @Override
    protected Type visitDecimalLiteral(DecimalLiteral node, Context context) {
      return addType(node, new NumberType());
    }

    @Override
    protected Type visitGenericLiteral(GenericLiteral node, Context context) {
      throw new RuntimeException("Generic literal not supported yet.");
    }

    @Override
    protected Type visitTimestampLiteral(TimestampLiteral node, Context context) {
      return addType(node, new DateTimeType());
    }

    @Override
    protected Type visitIntervalLiteral(IntervalLiteral node, Context context) {
      return addType(node, new DateTimeType());
    }

    @Override
    protected Type visitStringLiteral(StringLiteral node, Context context) {
      return addType(node, new StringType());
    }

    @Override
    protected Type visitBooleanLiteral(BooleanLiteral node, Context context) {
      return addType(node, new BooleanType());
    }

    @Override
    protected Type visitEnumLiteral(EnumLiteral node, Context context) {
      throw new RuntimeException("Enum literal not supported yet.");
    }

    @Override
    protected Type visitNullLiteral(NullLiteral node, Context context) {
      return addType(node, new NullType());
    }

    @Override
    protected Type visitLongLiteral(LongLiteral node, Context context) {
      return addType(node, new NumberType());
    }

    @Override
    protected Type visitDereferenceExpression(DereferenceExpression node, Context context) {
      Type type = node.getBase().accept(this, context);
      if (!(type instanceof RelationType)) {
        throw new RuntimeException(String.format("Dereference type not a relation: %s", node));
      }
      RelationType relType = (RelationType) type;
      Optional<Field> field = relType.getField(node.getField().getValue());
      if (field.isEmpty()) {
        throw new RuntimeException(String.format("Could not dereference %s in %s", node.getBase(), node.getField()));
      }

      return addType(node, field.get().getType());
    }

    @Override
    public Type visitIsEmpty(IsEmpty node, Context context) {
      //tbd
      return addType(node, new StringType());
    }

    private Type getOperator(Context context, Expression node, OperatorType operatorType, Expression... arguments)
    {
      ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
      for (Expression expression : arguments) {
        argumentTypes.add(process(expression, context));
      }
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
      return addType(node, new BooleanType());
    }

    private Type addType(Expression node, Type type) {
      analysis.addType(node, type);
      return type;
    }

    private void addRelation(Relation relation, RelationType type) {
      analysis.setRelation(relation, type);
    }
  }
}
