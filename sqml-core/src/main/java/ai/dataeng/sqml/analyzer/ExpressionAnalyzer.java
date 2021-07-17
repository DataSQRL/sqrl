/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.dataeng.sqml.analyzer;

import static ai.dataeng.sqml.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.MULTIPLE_FIELDS_FROM_SUBQUERY;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static ai.dataeng.sqml.analyzer.TypeSignatureProvider.fromTypes;
import static ai.dataeng.sqml.common.OperatorType.SUBSCRIPT;
import static ai.dataeng.sqml.common.type.BigintType.BIGINT;
import static ai.dataeng.sqml.common.type.BooleanType.BOOLEAN;
import static ai.dataeng.sqml.common.type.DateType.DATE;
import static ai.dataeng.sqml.common.type.DoubleType.DOUBLE;
import static ai.dataeng.sqml.common.type.IntegerType.INTEGER;
import static ai.dataeng.sqml.common.type.TimeType.TIME;
import static ai.dataeng.sqml.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static ai.dataeng.sqml.common.type.TimestampType.TIMESTAMP;
import static ai.dataeng.sqml.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static ai.dataeng.sqml.common.type.TypeSignature.parseTypeSignature;
import static ai.dataeng.sqml.common.type.UnknownType.UNKNOWN;
import static ai.dataeng.sqml.common.type.VarbinaryType.VARBINARY;
import static ai.dataeng.sqml.common.type.VarcharType.VARCHAR;
import static ai.dataeng.sqml.sql.tree.Extract.Field.TIMEZONE_HOUR;
import static ai.dataeng.sqml.sql.tree.Extract.Field.TIMEZONE_MINUTE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.common.OperatorType;
import ai.dataeng.sqml.common.PrestoException;
import ai.dataeng.sqml.common.QualifiedObjectName;
import ai.dataeng.sqml.common.StandardErrorCode;
import ai.dataeng.sqml.common.type.CharType;
import ai.dataeng.sqml.common.type.DecimalParseResult;
import ai.dataeng.sqml.common.type.Decimals;
import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.common.type.VarcharType;
import ai.dataeng.sqml.function.FunctionHandle;
import ai.dataeng.sqml.function.FunctionMetadata;
import ai.dataeng.sqml.metadata.FunctionAndTypeManager;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.metadata.OperatorNotFoundException;
import ai.dataeng.sqml.planner.TypeProvider;
import ai.dataeng.sqml.sql.parser.SqlParser;
import ai.dataeng.sqml.sql.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.sql.tree.ArithmeticUnaryExpression;
import ai.dataeng.sqml.sql.tree.ArrayConstructor;
import ai.dataeng.sqml.sql.tree.AtTimeZone;
import ai.dataeng.sqml.sql.tree.BetweenPredicate;
import ai.dataeng.sqml.sql.tree.BinaryLiteral;
import ai.dataeng.sqml.sql.tree.BooleanLiteral;
import ai.dataeng.sqml.sql.tree.Cast;
import ai.dataeng.sqml.sql.tree.CharLiteral;
import ai.dataeng.sqml.sql.tree.ComparisonExpression;
import ai.dataeng.sqml.sql.tree.CurrentTime;
import ai.dataeng.sqml.sql.tree.CurrentUser;
import ai.dataeng.sqml.sql.tree.DecimalLiteral;
import ai.dataeng.sqml.sql.tree.DereferenceExpression;
import ai.dataeng.sqml.sql.tree.DoubleLiteral;
import ai.dataeng.sqml.sql.tree.EnumLiteral;
import ai.dataeng.sqml.sql.tree.ExistsPredicate;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.Extract;
import ai.dataeng.sqml.sql.tree.FieldReference;
import ai.dataeng.sqml.sql.tree.FunctionCall;
import ai.dataeng.sqml.sql.tree.GenericLiteral;
import ai.dataeng.sqml.sql.tree.GroupingOperation;
import ai.dataeng.sqml.sql.tree.Identifier;
import ai.dataeng.sqml.sql.tree.InListExpression;
import ai.dataeng.sqml.sql.tree.InPredicate;
import ai.dataeng.sqml.sql.tree.IntervalLiteral;
import ai.dataeng.sqml.sql.tree.IsNotNullPredicate;
import ai.dataeng.sqml.sql.tree.IsNullPredicate;
import ai.dataeng.sqml.sql.tree.LikePredicate;
import ai.dataeng.sqml.sql.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.sql.tree.LongLiteral;
import ai.dataeng.sqml.sql.tree.Node;
import ai.dataeng.sqml.sql.tree.NodeRef;
import ai.dataeng.sqml.sql.tree.NotExpression;
import ai.dataeng.sqml.sql.tree.NullLiteral;
import ai.dataeng.sqml.sql.tree.Parameter;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import ai.dataeng.sqml.sql.tree.QuantifiedComparisonExpression;
import ai.dataeng.sqml.sql.tree.Row;
import ai.dataeng.sqml.sql.tree.SearchedCaseExpression;
import ai.dataeng.sqml.sql.tree.SimpleCaseExpression;
import ai.dataeng.sqml.sql.tree.SortItem;
import ai.dataeng.sqml.sql.tree.StackableAstVisitor;
import ai.dataeng.sqml.sql.tree.StringLiteral;
import ai.dataeng.sqml.sql.tree.SubqueryExpression;
import ai.dataeng.sqml.sql.tree.SubscriptExpression;
import ai.dataeng.sqml.sql.tree.SymbolReference;
import ai.dataeng.sqml.sql.tree.TimeLiteral;
import ai.dataeng.sqml.sql.tree.TimestampLiteral;
import ai.dataeng.sqml.sql.tree.WhenClause;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.slice.SliceUtf8;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;

public class ExpressionAnalyzer {

  private static final int MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT = 63;
  private static final int MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER = 31;

  private final FunctionAndTypeManager functionAndTypeManager;
  private final Function<Node, StatementAnalyzer> statementAnalyzerFactory;
  private final TypeProvider symbolTypes;

  private final Map<NodeRef<FunctionCall>, FunctionHandle> resolvedFunctions = new LinkedHashMap<>();
  private final Set<NodeRef<SubqueryExpression>> scalarSubqueries = new LinkedHashSet<>();
  private final Set<NodeRef<ExistsPredicate>> existsSubqueries = new LinkedHashSet<>();
  private final Map<NodeRef<Expression>, Type> expressionCoercions = new LinkedHashMap<>();
  private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
  private final Set<NodeRef<InPredicate>> subqueryInPredicates = new LinkedHashSet<>();
  private final Map<NodeRef<Expression>, FieldId> columnReferences = new LinkedHashMap<>();
  private final Map<NodeRef<Expression>, Type> expressionTypes = new LinkedHashMap<>();
  private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons = new LinkedHashSet<>();
  // For lambda argument references, maps each QualifiedNameReference to the referenced LambdaArgumentDeclaration
  private final Multimap<QualifiedObjectName, String> tableColumnReferences = HashMultimap.create();

//  private final Optional<TransactionId> transactionId;
//  private final Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions;
  private final List<Expression> parameters;

  private ExpressionAnalyzer(
      FunctionAndTypeManager functionAndTypeManager,
      Function<Node, StatementAnalyzer> statementAnalyzerFactory,
      TypeProvider symbolTypes,
      List<Expression> parameters) {
    this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory,
        "statementAnalyzerFactory is null");
    this.symbolTypes = requireNonNull(symbolTypes, "symbolTypes is null");
    this.parameters = requireNonNull(parameters, "parameters is null");
  }

  public static FunctionHandle resolveFunction(
      FunctionCall node,
      List<TypeSignatureProvider> argumentTypes,
      FunctionAndTypeManager functionAndTypeManager) {
    try {
      return functionAndTypeManager
          .resolveFunction(node.getName(),
              argumentTypes);
    } catch (PrestoException e) {
      if (e.getErrorCode().getCode() == StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode()
          .getCode()) {
        throw new SemanticException(
            SemanticErrorCode.FUNCTION_NOT_FOUND, node,
            e.getMessage());
      }
      if (e.getErrorCode().getCode() == StandardErrorCode.AMBIGUOUS_FUNCTION_CALL.toErrorCode()
          .getCode()) {
        throw new SemanticException(
            SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL, node,
            e.getMessage());
      }
      throw e;
    }
  }


  public static ExpressionAnalysis analyzeExpression(
      Session session,
      Metadata metadata,
      SqlParser sqlParser,
      Scope scope,
      Analysis analysis,
      Expression expression) {
    ExpressionAnalyzer analyzer = create(analysis, session, metadata, sqlParser, TypeProvider.empty());
    analyzer.analyze(expression, scope);

    Map<NodeRef<Expression>, Type> expressionTypes = analyzer.getExpressionTypes();
    Map<NodeRef<Expression>, Type> expressionCoercions = analyzer.getExpressionCoercions();
    Set<NodeRef<Expression>> typeOnlyCoercions = analyzer.getTypeOnlyCoercions();
    Map<NodeRef<FunctionCall>, FunctionHandle> resolvedFunctions = analyzer.getResolvedFunctions();

    analysis.addTypes(expressionTypes);
    analysis.addCoercions(expressionCoercions, typeOnlyCoercions);
    analysis.addFunctionHandles(resolvedFunctions);
    analysis.addColumnReferences(analyzer.getColumnReferences());
    analysis.addTableColumnReferences(analyzer.getTableColumnReferences());

    return new ExpressionAnalysis(
        expressionTypes,
        analyzer.getSubqueryInPredicates(),
        analyzer.getScalarSubqueries(),
        analyzer.getExistsSubqueries(),
        analyzer.getQuantifiedComparisons());
  }

  private static ExpressionAnalyzer create(
      Analysis analysis,
      Session session,
      Metadata metadata,
      SqlParser sqlParser,
      TypeProvider types) {
    return new ExpressionAnalyzer(
        metadata.getFunctionAndTypeManager(),
        node -> new StatementAnalyzer(analysis, metadata,
            sqlParser, session),
        types,
        analysis.getParameters());
  }

  public Map<NodeRef<FunctionCall>, FunctionHandle> getResolvedFunctions() {
    return unmodifiableMap(resolvedFunctions);
  }

  public Map<NodeRef<Expression>, Type> getExpressionTypes() {
    return unmodifiableMap(expressionTypes);
  }

  public Type setExpressionType(Expression expression, Type type) {
    requireNonNull(expression, "expression cannot be null");
    requireNonNull(type, "type cannot be null");

    expressionTypes.put(NodeRef.of(expression), type);

    return type;
  }

  private Type getExpressionType(Expression expression) {
    requireNonNull(expression, "expression cannot be null");

    Type type = expressionTypes.get(NodeRef.of(expression));
    checkState(type != null, "Expression not yet analyzed: %s", expression);
    return type;
  }

  public Map<NodeRef<Expression>, Type> getExpressionCoercions() {
    return unmodifiableMap(expressionCoercions);
  }

  public Set<NodeRef<Expression>> getTypeOnlyCoercions() {
    return unmodifiableSet(typeOnlyCoercions);
  }

  public Set<NodeRef<InPredicate>> getSubqueryInPredicates() {
    return unmodifiableSet(subqueryInPredicates);
  }

  public Map<NodeRef<Expression>, FieldId> getColumnReferences() {
    return unmodifiableMap(columnReferences);
  }

  public Type analyze(Expression expression, Scope scope) {
    Visitor visitor = new Visitor(scope);
    return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(
        Context.notInLambda(scope)));
  }

  private Type analyze(Expression expression, Scope baseScope, Context context) {
    Visitor visitor = new Visitor(baseScope);
    return visitor
        .process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(context));
  }

  public Set<NodeRef<SubqueryExpression>> getScalarSubqueries() {
    return unmodifiableSet(scalarSubqueries);
  }

  public Set<NodeRef<ExistsPredicate>> getExistsSubqueries() {
    return unmodifiableSet(existsSubqueries);
  }

  public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons() {
    return unmodifiableSet(quantifiedComparisons);
  }

  public Multimap<QualifiedObjectName, String> getTableColumnReferences() {
    return tableColumnReferences;
  }

  private static class Context {

    private final Scope scope;

    // functionInputTypes and nameToLambdaDeclarationMap can be null or non-null independently. All 4 combinations are possible.

    // The list of types when expecting a lambda (i.e. processing lambda parameters of a function); null otherwise.
    // Empty list represents expecting a lambda with no arguments.
    private final List<Type> functionInputTypes;
    // The mapping from names to corresponding lambda argument declarations when inside a lambda; null otherwise.
    // Empty map means that the all lambda expressions surrounding the current node has no arguments.

    private Context(
        Scope scope,
        List<Type> functionInputTypes) {
      this.scope = requireNonNull(scope, "scope is null");
      this.functionInputTypes = functionInputTypes;
    }

    public static Context notInLambda(Scope scope) {
      return new Context(scope, null);
    }

    Scope getScope() {
      return scope;
    }


    public List<Type> getFunctionInputTypes() {
      return functionInputTypes;
    }
  }

  private class Visitor
      extends StackableAstVisitor<Type, Context> {

    // Used to resolve FieldReferences (e.g. during local execution planning)
    private final Scope baseScope;

    public Visitor(Scope baseScope) {
      this.baseScope = requireNonNull(baseScope, "baseScope is null");
    }

    @Override
    public Type process(Node node, @Nullable StackableAstVisitorContext<Context> context) {
      if (node instanceof Expression) {
        // don't double process a node
        Type type = expressionTypes.get(NodeRef.of(((Expression) node)));
        if (type != null) {
          return type;
        }
      }
      return super.process(node, context);
    }

    @Override
    protected Type visitRow(Row node, StackableAstVisitorContext<Context> context) {
//      List<Type> types = node.getItems().stream()
//          .map((child) -> process(child, context))
//          .collect(toImmutableList());
//
//      Type type = RowType.anonymous(types);
//      return setExpressionType(node, type);
      return null;
    }

    @Override
    protected Type visitCurrentTime(CurrentTime node, StackableAstVisitorContext<Context> context) {
      if (node.getPrecision() != null) {
        throw new SemanticException(NOT_SUPPORTED, node,
            "non-default precision not yet supported");
      }

      Type type;
      switch (node.getFunction()) {
        case DATE:
          type = DATE;
          break;
        case TIME:
          type = TIME_WITH_TIME_ZONE;
          break;
        case LOCALTIME:
          type = TIME;
          break;
        case TIMESTAMP:
          type = TIMESTAMP_WITH_TIME_ZONE;
          break;
//        case LOCALTIMESTAMP:
//          type = TIMESTAMP;
//          break;
        default:
          throw new SemanticException(NOT_SUPPORTED, node,
              "%s not yet supported", node.getFunction().getName());
      }

      return setExpressionType(node, type);
    }

    @Override
    protected Type visitSymbolReference(SymbolReference node,
        StackableAstVisitorContext<Context> context) {

      Type type = symbolTypes.get(node);
      return setExpressionType(node, type);
    }

    @Override
    protected Type visitIdentifier(Identifier node, StackableAstVisitorContext<Context> context) {
      ResolvedField resolvedField = context.getContext().getScope()
          .resolveField(node, QualifiedName.of(node.getValue()));
      return handleResolvedField(node, resolvedField, context);
    }

    private Type handleResolvedField(Expression node, ResolvedField resolvedField,
        StackableAstVisitorContext<Context> context) {
      return handleResolvedField(node, FieldId.from(resolvedField),
          resolvedField.getField(), context);
    }

    private Type handleResolvedField(Expression node,
        FieldId fieldId, Field field,
        StackableAstVisitorContext<Context> context) {

      if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
        tableColumnReferences.put(field.getOriginTable().get(), field.getOriginColumnName().get());
      }

      FieldId previous = columnReferences
          .put(NodeRef.of(node), fieldId);
      checkState(previous == null, "%s already known to refer to %s", node, previous);
      return setExpressionType(node, field.getType());
    }

    @Override
    protected Type visitDereferenceExpression(DereferenceExpression node,
        StackableAstVisitorContext<Context> context) {
//      QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);
//
//      // Handle qualified name
//      if (qualifiedName != null) {
//        // first, try to match it to a column name
//        Scope scope = context.getContext().getScope();
//        Optional<ResolvedField> resolvedField = scope.tryResolveField(node, qualifiedName);
//        if (resolvedField.isPresent()) {
//          return handleResolvedField(node, resolvedField.get(), context);
//        }
//        // otherwise, try to match it to an enum literal (eg Mood.HAPPY)
//        if (!scope.isColumnReference(qualifiedName)) {
//          Optional<TypeWithName> enumType = tryResolveEnumLiteralType(qualifiedName,
//              functionAndTypeManager);
//          if (enumType.isPresent()) {
//            setExpressionType(node.getBase(), enumType.get());
//            return setExpressionType(node, enumType.get());
//          }
//          throw missingAttributeException(node, qualifiedName);
//        }
//      }
//
//      Type baseType = process(node.getBase(), context);
//      if (!(baseType instanceof RowType)) {
//        throw new SemanticException(TYPE_MISMATCH, node.getBase(),
//            "Expression %s is not of type ROW", node.getBase());
//      }
//
//      RowType rowType = (RowType) baseType;
//      String fieldName = node.getField().getValue();
//
//      Type rowFieldType = null;
//      for (RowType.Field rowField : rowType.getFields()) {
//        if (fieldName.equalsIgnoreCase(rowField.getName().orElse(null))) {
//          rowFieldType = rowField.getType();
//          break;
//        }
//      }
//
//      if (sqlFunctionProperties.isLegacyRowFieldOrdinalAccessEnabled() && rowFieldType == null) {
//        OptionalInt rowIndex = parseAnonymousRowFieldOrdinalAccess(fieldName, rowType.getFields());
//        if (rowIndex.isPresent()) {
//          rowFieldType = rowType.getFields().get(rowIndex.getAsInt()).getType();
//        }
//      }
//
//      if (rowFieldType == null) {
//        throw missingAttributeException(node);
//      }
//
//      return setExpressionType(node, rowFieldType);
      return null;
    }

    @Override
    protected Type visitNotExpression(NotExpression node,
        StackableAstVisitorContext<Context> context) {
      coerceType(context, node.getValue(), BOOLEAN, "Value of logical NOT expression");

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node,
        StackableAstVisitorContext<Context> context) {
      coerceType(context, node.getLeft(), BOOLEAN, "Left side of logical expression");
      coerceType(context, node.getRight(), BOOLEAN, "Right side of logical expression");

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitComparisonExpression(ComparisonExpression node,
        StackableAstVisitorContext<Context> context) {
      OperatorType operatorType = OperatorType.valueOf(node.getOperator().name());
      return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
    }

    @Override
    protected Type visitIsNullPredicate(IsNullPredicate node,
        StackableAstVisitorContext<Context> context) {
      process(node.getValue(), context);

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitIsNotNullPredicate(IsNotNullPredicate node,
        StackableAstVisitorContext<Context> context) {
      process(node.getValue(), context);

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitSearchedCaseExpression(SearchedCaseExpression node,
        StackableAstVisitorContext<Context> context) {
      for (WhenClause whenClause : node.getWhenClauses()) {
        coerceType(context, whenClause.getOperand(), BOOLEAN, "CASE WHEN clause");
      }

      Type type = coerceToSingleType(context,
          "All CASE results must be the same type: %s",
          getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
      setExpressionType(node, type);

      for (WhenClause whenClause : node.getWhenClauses()) {
        Type whenClauseType = process(whenClause.getResult(), context);
        setExpressionType(whenClause, whenClauseType);
      }

      return type;
    }

    @Override
    protected Type visitSimpleCaseExpression(SimpleCaseExpression node,
        StackableAstVisitorContext<Context> context) {
      for (WhenClause whenClause : node.getWhenClauses()) {
        coerceToSingleType(context, whenClause,
            "CASE operand type does not match WHEN clause operand type: %s vs %s",
            node.getOperand(), whenClause.getOperand());
      }

      Type type = coerceToSingleType(context,
          "All CASE results must be the same type: %s",
          getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
      setExpressionType(node, type);

      for (WhenClause whenClause : node.getWhenClauses()) {
        Type whenClauseType = process(whenClause.getResult(), context);
        setExpressionType(whenClause, whenClauseType);
      }

      return type;
    }

    private List<Expression> getCaseResultExpressions(List<WhenClause> whenClauses,
        Optional<Expression> defaultValue) {
      List<Expression> resultExpressions = new ArrayList<>();
      for (WhenClause whenClause : whenClauses) {
        resultExpressions.add(whenClause.getResult());
      }
      defaultValue.ifPresent(resultExpressions::add);
      return resultExpressions;
    }

    @Override
    protected Type visitArithmeticUnary(ArithmeticUnaryExpression node,
        StackableAstVisitorContext<Context> context) {
      switch (node.getSign()) {
        case PLUS:
          Type type = process(node.getValue(), context);

          if (!type.equals(DOUBLE) && !type.equals(BIGINT) && !type
              .equals(INTEGER)) {
            // TODO: figure out a type-agnostic way of dealing with this. Maybe add a special unary operator
            // that types can chose to implement, or piggyback on the existence of the negation operator
            throw new SemanticException(TYPE_MISMATCH, node,
                "Unary '+' operator cannot by applied to %s type", type);
          }
          return setExpressionType(node, type);
        case MINUS:
          return getOperator(context, node, OperatorType.NEGATION, node.getValue());
      }

      throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
    }

    @Override
    protected Type visitArithmeticBinary(ArithmeticBinaryExpression node,
        StackableAstVisitorContext<Context> context) {
      return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()),
          node.getLeft(), node.getRight());
    }

    @Override
    protected Type visitLikePredicate(LikePredicate node,
        StackableAstVisitorContext<Context> context) {
      Type valueType = process(node.getValue(), context);
      if (!(valueType instanceof CharType) && !(valueType instanceof VarcharType)) {
        coerceType(context, node.getValue(), VARCHAR, "Left side of LIKE expression");
      }

      Type patternType = getVarcharType(node.getPattern(), context);
      coerceType(context, node.getPattern(), patternType, "Pattern for LIKE expression");

      return setExpressionType(node, BOOLEAN);
    }

    private Type getVarcharType(Expression value, StackableAstVisitorContext<Context> context) {
      Type type = process(value, context);
      if (!(type instanceof VarcharType)) {
        return VARCHAR;
      }
      return type;
    }

    @Override
    protected Type visitSubscriptExpression(SubscriptExpression node,
        StackableAstVisitorContext<Context> context) {
      Type baseType = process(node.getBase(), context);
      // Subscript on Row hasn't got a dedicated operator. Its Type is resolved by hand.
//      if (baseType instanceof RowType) {
//        if (!(node.getIndex() instanceof LongLiteral)) {
//          throw new SemanticException(
//              INVALID_PARAMETER_USAGE,
//              node.getIndex(),
//              "Subscript expression on ROW requires a constant index");
//        }
//        Type indexType = process(node.getIndex(), context);
//        if (!indexType.equals(INTEGER)) {
//          throw new SemanticException(
//              TYPE_MISMATCH,
//              node.getIndex(),
//              "Subscript expression on ROW requires integer index, found %s", indexType);
//        }
//        int indexValue = toIntExact(((LongLiteral) node.getIndex()).getValue());
//        if (indexValue <= 0) {
//          throw new SemanticException(
//              INVALID_PARAMETER_USAGE,
//              node.getIndex(),
//              "Invalid subscript index: %s. ROW indices start at 1", indexValue);
//        }
//        List<Type> rowTypes = baseType.getTypeParameters();
//        if (indexValue > rowTypes.size()) {
//          throw new SemanticException(
//              INVALID_PARAMETER_USAGE,
//              node.getIndex(),
//              "Subscript index out of bounds: %s, max value is %s", indexValue, rowTypes.size());
//        }
//        return setExpressionType(node, rowTypes.get(indexValue - 1));
//      }
      return getOperator(context, node, SUBSCRIPT, node.getBase(), node.getIndex());
    }

    @Override
    protected Type visitArrayConstructor(ArrayConstructor node,
        StackableAstVisitorContext<Context> context) {
//      Type type = coerceToSingleType(context, "All ARRAY elements must be the same type: %s",
//          node.getValues());
//      Type arrayType = functionAndTypeManager.getParameterizedType(ARRAY.getName(),
//          ImmutableList.of(TypeSignatureParameter.of(type.getTypeSignature())));
//      return setExpressionType(node, arrayType);
      return null;
    }

    @Override
    protected Type visitStringLiteral(StringLiteral node,
        StackableAstVisitorContext<Context> context) {
      VarcharType type = VarcharType.createVarcharType(SliceUtf8.countCodePoints(node.getSlice()));
      return setExpressionType(node, type);
    }

    @Override
    protected Type visitCharLiteral(CharLiteral node, StackableAstVisitorContext<Context> context) {
      CharType type = CharType.createCharType(node.getValue().length());
      return setExpressionType(node, type);
    }

    @Override
    protected Type visitBinaryLiteral(BinaryLiteral node,
        StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, VARBINARY);
    }

    @Override
    protected Type visitLongLiteral(LongLiteral node, StackableAstVisitorContext<Context> context) {
      if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
        return setExpressionType(node, INTEGER);
      }

      return setExpressionType(node, BIGINT);
    }

    @Override
    protected Type visitDoubleLiteral(DoubleLiteral node,
        StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, DOUBLE);
    }

    @Override
    protected Type visitDecimalLiteral(DecimalLiteral node,
        StackableAstVisitorContext<Context> context) {
      DecimalParseResult parseResult = Decimals.parse(node.getValue());
      return setExpressionType(node, parseResult.getType());
    }

    @Override
    protected Type visitBooleanLiteral(BooleanLiteral node,
        StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitGenericLiteral(GenericLiteral node,
        StackableAstVisitorContext<Context> context) {
      Type type;
      try {
        type = functionAndTypeManager.getType(parseTypeSignature(node.getType()));
      } catch (IllegalArgumentException e) {
        throw new SemanticException(TYPE_MISMATCH, node,
            "Unknown type: " + node.getType());
      }

//      if (!JSON.equals(type)) {
//        try {
//          functionAndTypeManager
//              .lookupCast(CAST, VARCHAR.getTypeSignature(), type.getTypeSignature());
//        } catch (IllegalArgumentException e) {
//          throw new SemanticException(TYPE_MISMATCH, node,
//              "No literal form for type %s", type);
//        }
//      }

      return setExpressionType(node, type);
    }

    @Override
    protected Type visitEnumLiteral(EnumLiteral node, StackableAstVisitorContext<Context> context) {
      Type type;
      try {
        type = functionAndTypeManager.getType(parseTypeSignature(node.getType()));
      } catch (IllegalArgumentException e) {
        throw new SemanticException(TYPE_MISMATCH, node,
            "Unknown type: " + node.getType());
      }

      return setExpressionType(node, type);
    }

    @Override
    protected Type visitTimeLiteral(TimeLiteral node, StackableAstVisitorContext<Context> context) {
//      boolean hasTimeZone;
//      try {
//        hasTimeZone = timeHasTimeZone(node.getValue());
//      } catch (IllegalArgumentException e) {
//        throw new SemanticException(INVALID_LITERAL, node,
//            "'%s' is not a valid time literal", node.getValue());
//      }
//      Type type = hasTimeZone ? TIME_WITH_TIME_ZONE : TIME;
//      return setExpressionType(node, type);
      return null;
    }

    @Override
    protected Type visitTimestampLiteral(TimestampLiteral node,
        StackableAstVisitorContext<Context> context) {
//      try {
//        if (sqlFunctionProperties.isLegacyTimestamp()) {
//          parseTimestampLiteral(sqlFunctionProperties.getTimeZoneKey(), node.getValue());
//        } else {
//          parseTimestampLiteral(node.getValue());
//        }
//      } catch (Exception e) {
//        throw new SemanticException(INVALID_LITERAL, node,
//            "'%s' is not a valid timestamp literal", node.getValue());
//      }
//
//      Type type;
//      if (timestampHasTimeZone(node.getValue())) {
//        type = TIMESTAMP_WITH_TIME_ZONE;
//      } else {
//        type = TIMESTAMP;
//      }
//      return setExpressionType(node, type);
      return null;
    }

    @Override
    protected Type visitIntervalLiteral(IntervalLiteral node,
        StackableAstVisitorContext<Context> context) {
//      Type type;
//      if (node.isYearToMonth()) {
//        type = INTERVAL_YEAR_MONTH;
//      } else {
//        type = INTERVAL_DAY_TIME;
//      }
      return setExpressionType(node, UNKNOWN);
    }

    @Override
    protected Type visitNullLiteral(NullLiteral node, StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, UNKNOWN);
    }

    @Override
    protected Type visitFunctionCall(FunctionCall node,
        StackableAstVisitorContext<Context> context) {
      if (node.getFilter().isPresent()) {
        Expression expression = node.getFilter().get();
        process(expression, context);
      }

      ImmutableList.Builder<TypeSignatureProvider> argumentTypesBuilder = ImmutableList.builder();
      for (Expression expression : node.getArguments()) {
        argumentTypesBuilder
            .add(new TypeSignatureProvider(process(expression, context).getTypeSignature()));
      }

      ImmutableList<TypeSignatureProvider> argumentTypes = argumentTypesBuilder.build();
      FunctionHandle function = resolveFunction(node,
          argumentTypes, functionAndTypeManager);

      if (node.getOrderBy().isPresent()) {
        for (SortItem sortItem : node.getOrderBy().get().getSortItems()) {
          Type sortKeyType = process(sortItem.getSortKey(), context);
          if (!sortKeyType.isOrderable()) {
            throw new SemanticException(TYPE_MISMATCH, node,
                "ORDER BY can only be applied to orderable types (actual: %s)",
                sortKeyType.getDisplayName());
          }
        }
      }
// todo: type checking for arguments
//      for (int i = 0; i < node.getArguments().size(); i++) {
//        Expression expression = node.getArguments().get(i);
//        Type expectedType = functionAndTypeManager
//            .getType(functionMetadata.getArgumentTypes().get(i));
//        requireNonNull(expectedType,
//            format("Type %s not found", functionMetadata.getArgumentTypes().get(i)));
//        if (node.isDistinct() && !expectedType.isComparable()) {
//          throw new SemanticException(TYPE_MISMATCH, node,
//              "DISTINCT can only be applied to comparable types (actual: %s)", expectedType);
//        }
//        if (argumentTypes.get(i).hasDependency()) {
//          throw new RuntimeException("?");
////          FunctionType expectedFunctionType = (FunctionType) expectedType;
////          process(expression, new StackableAstVisitorContext<>(
////              context.getContext().expectingLambda(expectedFunctionType.getArgumentTypes())));
//        } else {
//          Type actualType = functionAndTypeManager.getType(argumentTypes.get(i).getTypeSignature());
//          coerceType(expression, actualType, expectedType,
//              format("Function %s argument %d", function, i));
//        }
//      }
      resolvedFunctions.put(NodeRef.of(node), function);

//      Type type = functionAndTypeManager.getType(functionMetadata.getReturnType());
      return setExpressionType(node, DOUBLE);
    }

    @Override
    protected Type visitAtTimeZone(AtTimeZone node, StackableAstVisitorContext<Context> context) {
      Type valueType = process(node.getValue(), context);
      process(node.getTimeZone(), context);
      if (!valueType.equals(TIME_WITH_TIME_ZONE) && !valueType.equals(TIMESTAMP_WITH_TIME_ZONE)
          && !valueType.equals(TIME) && !valueType.equals(TIMESTAMP)) {
        throw new SemanticException(TYPE_MISMATCH, node.getValue(),
            "Type of value must be a time or timestamp with or without time zone (actual %s)",
            valueType);
      }
      Type resultType = valueType;
      if (valueType.equals(TIME)) {
        resultType = TIME_WITH_TIME_ZONE;
      } else if (valueType.equals(TIMESTAMP)) {
        resultType = TIMESTAMP_WITH_TIME_ZONE;
      }

      return setExpressionType(node, resultType);
    }

    @Override
    protected Type visitCurrentUser(CurrentUser node, StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, VARCHAR);
    }

    @Override
    protected Type visitParameter(Parameter node, StackableAstVisitorContext<Context> context) {
      if (parameters.size() == 0) {
        throw new SemanticException(INVALID_PARAMETER_USAGE, node,
            "query takes no parameters");
      }
      if (node.getPosition() >= parameters.size()) {
        throw new SemanticException(INVALID_PARAMETER_USAGE, node,
            "invalid parameter index %s, max value is %s", node.getPosition(),
            parameters.size() - 1);
      }

      Type resultType = process(parameters.get(node.getPosition()), context);
      return setExpressionType(node, resultType);
    }

    @Override
    protected Type visitExtract(Extract node, StackableAstVisitorContext<Context> context) {
      Type type = process(node.getExpression(), context);
      if (!isDateTimeType(type)) {
        throw new SemanticException(TYPE_MISMATCH,
            node.getExpression(),
            "Type of argument to extract must be DATE, TIME, TIMESTAMP, or INTERVAL (actual %s)",
            type);
      }
      Extract.Field field = node.getField();
      if ((field == TIMEZONE_HOUR || field == TIMEZONE_MINUTE) && !(type.equals(TIME_WITH_TIME_ZONE)
          || type.equals(TIMESTAMP_WITH_TIME_ZONE))) {
        throw new SemanticException(TYPE_MISMATCH,
            node.getExpression(),
            "Type of argument to extract time zone field must have a time zone (actual %s)", type);
      }

      return setExpressionType(node, BIGINT);
    }

    private boolean isDateTimeType(Type type) {
      return type.equals(DATE) ||
          type.equals(TIME) ||
          type.equals(TIME_WITH_TIME_ZONE) ||
          type.equals(TIMESTAMP) ||
          type.equals(TIMESTAMP_WITH_TIME_ZONE);
    }

    @Override
    protected Type visitBetweenPredicate(BetweenPredicate node,
        StackableAstVisitorContext<Context> context) {
      return getOperator(context, node, OperatorType.BETWEEN, node.getValue(), node.getMin(),
          node.getMax());
    }

    @Override
    public Type visitCast(Cast node, StackableAstVisitorContext<Context> context) {
      Type type;
      try {
        type = functionAndTypeManager.getType(parseTypeSignature(node.getType()));
      } catch (IllegalArgumentException e) {
        throw new SemanticException(TYPE_MISMATCH, node,
            "Unknown type: " + node.getType());
      }

      if (type.equals(UNKNOWN)) {
        throw new SemanticException(TYPE_MISMATCH, node,
            "UNKNOWN is not a valid type");
      }

      Type value = process(node.getExpression(), context);
      if (!value.equals(UNKNOWN) && !node.isTypeOnly()) {
        try {
//          functionAndTypeManager
//              .lookupCast(CAST, value.getTypeSignature(), type.getTypeSignature());
        } catch (OperatorNotFoundException e) {
          throw new SemanticException(TYPE_MISMATCH, node,
              "Cannot cast %s to %s", value, type);
        }
      }

      return setExpressionType(node, type);
    }

    @Override
    protected Type visitInPredicate(InPredicate node, StackableAstVisitorContext<Context> context) {
      Expression value = node.getValue();
      process(value, context);

      Expression valueList = node.getValueList();
      process(valueList, context);

      if (valueList instanceof InListExpression) {
        InListExpression inListExpression = (InListExpression) valueList;

        coerceToSingleType(context,
            "IN value and list items must be the same type: %s",
            ImmutableList.<Expression>builder().add(value).addAll(inListExpression.getValues())
                .build());
      } else if (valueList instanceof SubqueryExpression) {
        coerceToSingleType(context, node,
            "value and result of subquery must be of the same type for IN expression: %s vs %s",
            value, valueList);
      }

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitInListExpression(InListExpression node,
        StackableAstVisitorContext<Context> context) {
      Type type = coerceToSingleType(context, "All IN list values must be the same type: %s",
          node.getValues());

      setExpressionType(node, type);
      return type; // TODO: this really should a be relation type
    }

    @Override
    protected Type visitSubqueryExpression(SubqueryExpression node,
        StackableAstVisitorContext<Context> context) {

      StatementAnalyzer analyzer = statementAnalyzerFactory
          .apply(node);
      Scope subqueryScope = Scope.builder()
          .withParent(context.getContext().getScope())
          .build();
      Scope queryScope = analyzer.analyze(node.getQuery(), subqueryScope);

      // Subquery should only produce one column
      if (queryScope.getRelationType().getVisibleFieldCount() != 1) {
        throw new SemanticException(MULTIPLE_FIELDS_FROM_SUBQUERY,
            node,
            "Multiple columns returned by subquery are not yet supported. Found %s",
            queryScope.getRelationType().getVisibleFieldCount());
      }

      Node previousNode = context.getPreviousNode().orElse(null);
      if (previousNode instanceof InPredicate && ((InPredicate) previousNode).getValue() != node) {
        subqueryInPredicates.add(NodeRef.of((InPredicate) previousNode));
      } else if (previousNode instanceof QuantifiedComparisonExpression) {
        quantifiedComparisons.add(NodeRef.of((QuantifiedComparisonExpression) previousNode));
      } else {
        scalarSubqueries.add(NodeRef.of(node));
      }

      Type type = getOnlyElement(queryScope.getRelationType().getVisibleFields()).getType();
      return setExpressionType(node, type);
    }

    @Override
    protected Type visitExists(ExistsPredicate node, StackableAstVisitorContext<Context> context) {
      StatementAnalyzer analyzer = statementAnalyzerFactory
          .apply(node);
      Scope subqueryScope = Scope.builder().withParent(context.getContext().getScope()).build();
      analyzer.analyze(node.getSubquery(), subqueryScope);

      existsSubqueries.add(NodeRef.of(node));

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node,
        StackableAstVisitorContext<Context> context) {
      Expression value = node.getValue();
      process(value, context);

      Expression subquery = node.getSubquery();
      process(subquery, context);

      Type comparisonType = coerceToSingleType(context, node,
          "Value expression and result of subquery must be of the same type for quantified comparison: %s vs %s",
          value, subquery);

      switch (node.getOperator()) {
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
          if (!comparisonType.isOrderable()) {
            throw new SemanticException(TYPE_MISMATCH, node,
                "Type [%s] must be orderable in order to be used in quantified comparison",
                comparisonType);
          }
          break;
        case EQUAL:
        case NOT_EQUAL:
          if (!comparisonType.isComparable()) {
            throw new SemanticException(TYPE_MISMATCH, node,
                "Type [%s] must be comparable in order to be used in quantified comparison",
                comparisonType);
          }
          break;
        default:
          throw new IllegalStateException(
              format("Unexpected comparison type: %s", node.getOperator()));
      }

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    public Type visitFieldReference(FieldReference node,
        StackableAstVisitorContext<Context> context) {
      Field field = baseScope.getRelationType().getFieldByIndex(node.getFieldIndex());
      return handleResolvedField(node,
          new FieldId(baseScope.getRelationId(),
              node.getFieldIndex()), field, context);
    }

    @Override
    protected Type visitExpression(Expression node, StackableAstVisitorContext<Context> context) {
      throw new SemanticException(NOT_SUPPORTED, node,
          "not yet implemented: " + node.getClass().getName());
    }

    @Override
    protected Type visitNode(Node node, StackableAstVisitorContext<Context> context) {
      throw new SemanticException(NOT_SUPPORTED, node,
          "not yet implemented: " + node.getClass().getName());
    }

    @Override
    public Type visitGroupingOperation(GroupingOperation node,
        StackableAstVisitorContext<Context> context) {
      if (node.getGroupingColumns().size() > MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT) {
        throw new SemanticException(INVALID_PROCEDURE_ARGUMENTS,
            node, String.format("GROUPING supports up to %d column arguments",
            MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT));
      }

      for (Expression columnArgument : node.getGroupingColumns()) {
        process(columnArgument, context);
      }

      if (node.getGroupingColumns().size() <= MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER) {
        return setExpressionType(node, INTEGER);
      } else {
        return setExpressionType(node, BIGINT);
      }
    }

    private Type getOperator(StackableAstVisitorContext<Context> context, Expression node,
        OperatorType operatorType, Expression... arguments) {
      ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
      for (Expression expression : arguments) {
        Type type = process(expression, context);
        if (type == null) {
          System.out.println();
        }
        argumentTypes.add(type);
      }

      FunctionMetadata operatorMetadata;
      try {
        operatorMetadata = functionAndTypeManager.getFunctionMetadata(
            functionAndTypeManager.resolveOperator(operatorType, fromTypes(argumentTypes.build())));
      } catch (OperatorNotFoundException e) {
        throw new SemanticException(TYPE_MISMATCH, node, "%s",
            e.getMessage());
      } catch (PrestoException e) {
        if (e.getErrorCode().getCode() == StandardErrorCode.AMBIGUOUS_FUNCTION_CALL.toErrorCode()
            .getCode()) {
          throw new SemanticException(
              SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL, node,
              e.getMessage());
        }
        throw e;
      }

      for (int i = 0; i < arguments.length; i++) {
        if (operatorMetadata.getArgumentTypes().size() >= i) continue; //todo fix function resolution
        Expression expression = arguments[i];
        Type type = functionAndTypeManager.getType(operatorMetadata.getArgumentTypes().get(i));
        coerceType(context, expression, type,
            format("Operator %s argument %d", operatorMetadata, i));
      }

      Type type = functionAndTypeManager.getType(operatorMetadata.getReturnType());
      return setExpressionType(node, type);
    }

    private void coerceType(Expression expression, Type actualType, Type expectedType,
        String message) {
      if (!actualType.equals(expectedType)) {
        if (!functionAndTypeManager.canCoerce(actualType, expectedType)) {
          throw new SemanticException(TYPE_MISMATCH, expression,
              message + " must evaluate to a %s (actual: %s)", expectedType, actualType);
        }
        addOrReplaceExpressionCoercion(expression, actualType, expectedType);
      }
    }

    private void coerceType(StackableAstVisitorContext<Context> context, Expression expression,
        Type expectedType, String message) {
      Type actualType = process(expression, context);
      coerceType(expression, actualType, expectedType, message);
    }

    private Type coerceToSingleType(StackableAstVisitorContext<Context> context, Node node,
        String message, Expression first, Expression second) {
      Type firstType = UNKNOWN;
      if (first != null) {
        firstType = process(first, context);
      }
      Type secondType = UNKNOWN;
      if (second != null) {
        secondType = process(second, context);
      }

      // coerce types if possible
      Optional<Type> superTypeOptional = functionAndTypeManager
          .getCommonSuperType(firstType, secondType);
      if (superTypeOptional.isPresent()
          && functionAndTypeManager.canCoerce(firstType, superTypeOptional.get())
          && functionAndTypeManager.canCoerce(secondType, superTypeOptional.get())) {
        Type superType = superTypeOptional.get();
        if (!firstType.equals(superType)) {
          addOrReplaceExpressionCoercion(first, firstType, superType);
        }
        if (!secondType.equals(superType)) {
          addOrReplaceExpressionCoercion(second, secondType, superType);
        }
        return superType;
      }

      throw new SemanticException(TYPE_MISMATCH, node, message,
          firstType, secondType);
    }

    private Type coerceToSingleType(StackableAstVisitorContext<Context> context, String message,
        List<Expression> expressions) {
      // determine super type
      Type superType = UNKNOWN;
      for (Expression expression : expressions) {
        Optional<Type> newSuperType = functionAndTypeManager
            .getCommonSuperType(superType, process(expression, context));
        if (!newSuperType.isPresent()) {
          throw new SemanticException(TYPE_MISMATCH, expression,
              message, superType.getDisplayName());
        }
        superType = newSuperType.get();
      }

      // verify all expressions can be coerced to the superType
      for (Expression expression : expressions) {
        Type type = process(expression, context);
        if (!type.equals(superType)) {
          if (!functionAndTypeManager.canCoerce(type, superType)) {
            throw new SemanticException(TYPE_MISMATCH, expression,
                message, superType.getDisplayName());
          }
          addOrReplaceExpressionCoercion(expression, type, superType);
        }
      }

      return superType;
    }

    private void addOrReplaceExpressionCoercion(Expression expression, Type type, Type superType) {
      NodeRef<Expression> ref = NodeRef.of(expression);
      expressionCoercions.put(ref, superType);
      if (functionAndTypeManager.isTypeOnlyCoercion(type, superType)) {
        typeOnlyCoercions.add(ref);
      } else
        typeOnlyCoercions.remove(ref);
    }
  }
}
