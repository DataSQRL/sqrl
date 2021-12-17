package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.OperatorType.QualifiedObjectName;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.logical3.LogicalPlan;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.schema2.*;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.tree.*;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.google.common.collect.ImmutableList.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.dataeng.sqml.analyzer.AggregationAnalyzer.verifyOrderByAggregations;
import static ai.dataeng.sqml.analyzer.AggregationAnalyzer.verifySourceAggregations;
import static ai.dataeng.sqml.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static ai.dataeng.sqml.analyzer.ExpressionTreeUtils.extractExpressions;
import static ai.dataeng.sqml.logical3.LogicalPlan.Builder.unbox;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;

@Slf4j
public class StatementAnalyzer extends AstVisitor<Scope, Scope> {

  private final Metadata metadata;
  private final StatementAnalysis analysis;
  private final LogicalPlan.Builder planBuilder;

  public StatementAnalyzer(Metadata metadata,
      LogicalPlan.Builder planBuilder, Statement statement) {
    this(metadata, new StatementAnalysis(statement, List.of(), false), planBuilder);
  }

  public StatementAnalyzer(Metadata metadata, StatementAnalysis statementAnalysis,
      LogicalPlan.Builder planBuilder) {
    this.metadata = metadata;
    this.analysis = statementAnalysis;
    this.planBuilder = planBuilder;
  }

  public StatementAnalysis getAnalysis() {
    return this.analysis;
  }

  @Override
  public Scope visitNode(Node node, Scope context) {
    throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
  }

  @Override
  public Scope visitQuery(Query node, Scope scope) {
    //Unions have a limit & order that is outside the query body. If these are empty, just process the
    //  query body as if it was a standalone query.
    Scope queryBodyScope = node.getQueryBody().accept(this, scope);
//    if (node.parseLimit().isEmpty() && node.getOrderBy().isEmpty()) {
//      return queryBodyScope;
//    }

    if (node.getOrderBy().isPresent()) {
      List<Expression> orderByExpressions = analyzeOrderBy(node, getSortItemsFromOrderBy(node.getOrderBy()), queryBodyScope);
      analysis.setOrderByExpressions(node, orderByExpressions);
    } else {
      analysis.setOrderByExpressions(node, List.of());
    }

    // Input fields == Output fields
    analysis.setOutputExpressions(node, descriptorToFields(queryBodyScope));

    return createAndAssignScope(node, scope, queryBodyScope.getRelation());
  }

  private List<Expression> descriptorToFields(Scope scope)
  {
    ImmutableList.Builder<Expression> builder = ImmutableList.builder();
    for (int fieldIndex = 0; fieldIndex < scope.getRelation().getAllFieldCount(); fieldIndex++) {
      FieldReference expression = new FieldReference(fieldIndex);
      builder.add(expression);
//      analyzeExpression(expression, scope);
    }
    return builder.build();
  }

  @Override
  public Scope visitQuerySpecification(QuerySpecification node, Scope scope) {
    Scope sourceScope = node.getFrom().accept(this, scope);

    node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

    List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
    List<Expression> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
    analyzeHaving(node, sourceScope);
    Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

    List<Expression> orderByExpressions = emptyList();
    Optional<Scope> orderByScope = Optional.empty();
    if (node.getOrderBy().isPresent()) {
      if (node.getSelect().isDistinct()) {
        verifySelectDistinct(node, outputExpressions);
      }

      OrderBy orderBy = node.getOrderBy().get();
      orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope, node));

      orderByExpressions = analyzeOrderBy(node, orderBy.getSortItems(), orderByScope.get());
    }


    Optional<Long> multiplicity = node.parseLimit();

    List<Expression> sourceExpressions = new ArrayList<>(outputExpressions);
    node.getHaving().ifPresent(sourceExpressions::add);

    analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
    List<FunctionCall> aggregates = analyzeAggregations(node, sourceExpressions, orderByExpressions);

    if (!aggregates.isEmpty() && groupByExpressions.isEmpty()) {
      // Have Aggregation functions but no explicit GROUP BY clause
      analysis.setGroupByExpressions(node, ImmutableList.of());
    }

    verifyAggregations(node, sourceScope, orderByScope, groupByExpressions, sourceExpressions, orderByExpressions);

//      analyzeWindowFunctions(node, outputExpressions, orderByExpressions);

    if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
      // Create a different scope for ORDER BY expressions when aggregation is present.
      // This is because planner requires scope in order to resolve names against fields.
      // Original ORDER BY scope "sees" FROM query fields. However, during planning
      // and when aggregation is present, ORDER BY expressions should only be resolvable against
      // output scope, group by expressions and aggregation expressions.
      List<GroupingOperation> orderByGroupingOperations = extractExpressions(orderByExpressions, GroupingOperation.class);
      List<FunctionCall> orderByAggregations = extractAggregateFunctions(orderByExpressions, metadata.getFunctionProvider());
      computeAndAssignOrderByScopeWithAggregation(node.getOrderBy().get(), sourceScope, outputScope, orderByAggregations, groupByExpressions, orderByGroupingOperations);
    }

    return outputScope;
  }

  @Override
  public Scope visitTable(Table table, Scope scope) {
    QualifiedName tableName = table.getName();
    TypedField field = planBuilder.resolveTableField(tableName, scope.getField())
        .orElseThrow(/*todo*/);
    Type type = unbox(field.getType());
    Preconditions.checkState(type instanceof RelationType);

    for (Field column : ((RelationType<?>) type).getFields()) {
      analysis.setColumn(field, new ColumnHandle(column));
    }

    return createAndAssignScope(table, scope, (RelationType) type);
  }

  @Override
  public Scope visitTableSubquery(TableSubquery node, Scope scope) {
    StatementAnalyzer statementAnalyzer = new StatementAnalyzer(metadata, this.analysis, planBuilder);
    Scope queryScope = node.getQuery().accept(statementAnalyzer, scope);
    return createAndAssignScope(node, scope, queryScope.getRelation());
  }

  @Override
  public Scope visitJoin(Join node, Scope scope) {
    Scope left = node.getLeft().accept(this, scope);
    Scope right = node.getRight().accept(this, scope);

    Scope result = createAndAssignScope(node, scope, join(left.getRelation(), right.getRelation()).getFields());

    //Todo verify that an empty criteria on a join can be a valid traversal
    if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT || node.getCriteria().isEmpty()) {
      return result;
    }

    JoinCriteria criteria = node.getCriteria().get();
    if (criteria instanceof JoinOn) {
      Expression expression = ((JoinOn) criteria).getExpression();

      // need to register coercions in case when join criteria requires coercion (e.g. join on char(1) = char(2))
      ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, result);
      Type clauseType = expressionAnalysis.getType(expression);
      if (!(clauseType instanceof BooleanType)) {
        throw new RuntimeException(String.format("JOIN ON clause must evaluate to a boolean: actual type %s", clauseType));
      }
      verifyNoAggregateGroupingFunctions(metadata.getFunctionProvider(), expression, "JOIN clause");

      //todo: restrict grouping criteria
//      analysis.setJoinCriteria(node, expression);
    } else {
      throw new RuntimeException("Unsupported join");
    }

    return result;
  }

  @Override
  public Scope visitAliasedRelation(AliasedRelation relation, Scope scope) {
    Scope relationScope = relation.getRelation().accept(this, scope);

    RelationType relationType = relationScope.getRelation();

    RelationType descriptor = relationType.withAlias(relation.getAlias().getValue());

    return createAndAssignScope(relation, scope, descriptor);
  }

  @Override
  public Scope visitUnion(Union node, Scope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public Scope visitIntersect(Intersect node, Scope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public Scope visitExcept(Except node, Scope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public Scope visitSetOperation(SetOperation node, Scope scope)
  {
    checkState(node.getRelations().size() >= 2);
    List<Scope> relationScopes = node.getRelations().stream()
        .map(relation -> {
          Scope relationScope = process(relation, scope);
          return createAndAssignScope(relation, scope, relationScope.getRelation());
        })
        .collect(toImmutableList());

    Type[] outputFieldTypes = relationScopes.get(0).getRelation().getFields().stream()
        .map(Field::getType)
        .toArray(Type[]::new);
    int outputFieldSize = outputFieldTypes.length;

    for (Scope relationScope : relationScopes) {
      RelationType relationType = relationScope.getRelation();
      int descFieldSize = relationType.getFields().size();
      String setOperationName = node.getClass().getSimpleName().toUpperCase(ENGLISH);
      if (outputFieldSize != descFieldSize) {
        throw new RuntimeException(
            String.format(
            "%s query has different number of fields: %d, %d",
            setOperationName,
            outputFieldSize,
            descFieldSize));
      }
      for (int i = 0; i < descFieldSize; i++) {
        /*
        Type descFieldType = relationType.getFieldByIndex(i).getType();
        Optional<Type> commonSuperType = metadata.getTypeManager().getCommonSuperType(outputFieldTypes[i], descFieldType);
        if (!commonSuperType.isPresent()) {
          throw new SemanticException(
              TYPE_MISMATCH,
              node,
              "column %d in %s query has incompatible types: %s, %s",
              i + 1,
              setOperationName,
              outputFieldTypes[i].getDisplayName(),
              descFieldType.getDisplayName());
        }
        outputFieldTypes[i] = commonSuperType.get();
         */
        //Super types?
      }
    }

    TypedField[] outputDescriptorFields = new TypedField[outputFieldTypes.length];
    RelationType firstDescriptor = relationScopes.get(0).getRelation();
    for (int i = 0; i < outputFieldTypes.length; i++) {
      Field oldField = (Field)firstDescriptor.getFields().get(i);
//      outputDescriptorFields[i] = new Field(
//          oldField.getRelationAlias(),
//          oldField.getName(),
//          outputFieldTypes[i],
//          oldField.isHidden(),
//          oldField.getOriginTable(),
//          oldField.getOriginColumnName(),
//          oldField.isAliased(), Optional.empty());
    }

    for (int i = 0; i < node.getRelations().size(); i++) {
      Relation relation = node.getRelations().get(i);
      Scope relationScope = relationScopes.get(i);
      RelationType relationType = relationScope.getRelation();
      for (int j = 0; j < relationType.getFields().size(); j++) {
        Type outputFieldType = outputFieldTypes[j];
        Type descFieldType = ((Field)relationType.getFields().get(j)).getType();
        if (!outputFieldType.equals(descFieldType)) {
//            analysis.addRelationCoercion(relation, outputFieldTypes);
          throw new RuntimeException(String.format("Mismatched types in set operation %s", relationType.getFields().get(j)));
//            break;
        }
      }
    }

    return createAndAssignScope(node, scope, new ArrayList<>(List.of(outputDescriptorFields)));
  }

  private void verifyAggregations(
      QuerySpecification node,
      Scope sourceScope,
      Optional<Scope> orderByScope,
      List<Expression> groupByExpressions,
      List<Expression> outputExpressions,
      List<Expression> orderByExpressions)
  {
    checkState(orderByExpressions.isEmpty() || orderByScope.isPresent(), "non-empty orderByExpressions list without orderByScope provided");

    if (analysis.isAggregation(node)) {
      // ensure SELECT, ORDER BY and HAVING are constant with respect to group
      // e.g, these are all valid expressions:
      //     SELECT f(a) GROUP BY a
      //     SELECT f(a + 1) GROUP BY a + 1
      //     SELECT a + sum(b) GROUP BY a
      List<Expression> distinctGroupingColumns = groupByExpressions.stream()
          .distinct()
          .collect(toImmutableList());

      for (Expression expression : outputExpressions) {
        verifySourceAggregations(distinctGroupingColumns, sourceScope, expression, metadata, analysis);
      }

      for (Expression expression : orderByExpressions) {
        verifyOrderByAggregations(distinctGroupingColumns, sourceScope, orderByScope.get(), expression, metadata, analysis);
      }
    }
  }

  private List<FunctionCall> analyzeAggregations(
      QuerySpecification node,
      List<Expression> outputExpressions,
      List<Expression> orderByExpressions)
  {
    List<FunctionCall> aggregates = extractAggregateFunctions(Iterables.concat(outputExpressions, orderByExpressions), metadata.getFunctionProvider());
    analysis.setAggregates(node, aggregates);
    return aggregates;
  }


  private void analyzeGroupingOperations(QuerySpecification node, List<Expression> outputExpressions, List<Expression> orderByExpressions)
  {
    List<GroupingOperation> groupingOperations = extractExpressions(Iterables.concat(outputExpressions, orderByExpressions), GroupingOperation.class);
    boolean isGroupingOperationPresent = !groupingOperations.isEmpty();

    if (isGroupingOperationPresent && !node.getGroupBy().isPresent()) {
      throw new RuntimeException(
          "A GROUPING() operation can only be used with a corresponding GROUPING SET/CUBE/ROLLUP/GROUP BY clause");
    }

    analysis.setGroupingOperations(node, groupingOperations);
  }

  private void verifySelectDistinct(QuerySpecification node, List<Expression> outputExpressions)
  {
    for (SortItem item : node.getOrderBy().get().getSortItems()) {
      Expression expression = item.getSortKey();


      Expression rewrittenOrderByExpression = ExpressionTreeRewriter.rewriteWith(
          new OrderByExpressionRewriter(extractNamedOutputExpressions(node.getSelect())), expression);
      int index = outputExpressions.indexOf(rewrittenOrderByExpression);
      if (index == -1) {
        throw new RuntimeException(String.format("For SELECT DISTINCT, ORDER BY expressions must appear in select list"));
      }
//        if (!isDeterministic(expression)) {
//          throw new SemanticException(NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT, expression, "Non deterministic ORDER BY expression is not supported with SELECT DISTINCT");
//        }
    }
  }

  private Multimap<QualifiedName, Expression> extractNamedOutputExpressions(Select node)
  {
    // Compute aliased output terms so we can resolve order by expressions against them first
    ImmutableMultimap.Builder<QualifiedName, Expression> assignments = ImmutableMultimap.builder();
    for (SelectItem item : node.getSelectItems()) {
      if (item instanceof SingleColumn) {
        SingleColumn column = (SingleColumn) item;
        Optional<Identifier> alias = column.getAlias();
        if (alias.isPresent()) {
          assignments.put(QualifiedName.of(alias.get().getValue()), column.getExpression()); // TODO: need to know if alias was quoted
        }
        else if (column.getExpression() instanceof Identifier) {
          assignments.put(QualifiedName.of(((Identifier) column.getExpression()).getValue()), column.getExpression());
        }
      }
    }

    return assignments.build();
  }

  private class OrderByExpressionRewriter
      extends ExpressionRewriter<Void>
  {
    private final Multimap<QualifiedName, Expression> assignments;

    public OrderByExpressionRewriter(Multimap<QualifiedName, Expression> assignments)
    {
      this.assignments = assignments;
    }

    @Override
    public Expression rewriteIdentifier(Identifier reference, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
      // if this is a simple name reference, try to resolve against output columns
      QualifiedName name = QualifiedName.of(reference.getValue());
      Set<Expression> expressions = assignments.get(name)
          .stream()
          .collect(Collectors.toSet());

      if (expressions.size() > 1) {
        throw new RuntimeException(String.format("'%s' in ORDER BY is ambiguous", name));
      }

      if (expressions.size() == 1) {
        return Iterables.getOnlyElement(expressions);
      }

      // otherwise, couldn't resolve name against output aliases, so fall through...
      return reference;
    }
  }

  public void analyzeWhere(Node node, Scope scope, Expression predicate) {
    ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);

    verifyNoAggregateGroupingFunctions(metadata.getFunctionProvider(), predicate, "WHERE clause");

//    analysis.recordSubqueries(node, expressionAnalysis);

    Type predicateType = expressionAnalysis.getType(predicate);
    if (!(predicateType instanceof BooleanType)) {
      throw new RuntimeException(String.format("WHERE clause must evaluate to a boolean: actual type %s", predicateType));
      // coerce null to boolean
//      analysis.addCoercion(predicate, BooleanType.INSTANCE, false);
    }

    analysis.setWhere(node, predicate);
  }

  private void verifyNoAggregateGroupingFunctions(FunctionProvider functionProvider, Expression predicate, String clause) {
    List<FunctionCall> aggregates = extractAggregateFunctions(ImmutableList.of(predicate), functionProvider);

    List<GroupingOperation> groupingOperations = extractExpressions(ImmutableList.of(predicate), GroupingOperation.class);

    List<Expression> found = ImmutableList.copyOf(Iterables.concat(
        aggregates,
        groupingOperations));

    if (!found.isEmpty()) {
      throw new RuntimeException(String.format(
          "%s cannot contain aggregations, window functions or grouping operations: %s", clause, found));
    }
  }

  private List<Expression> analyzeOrderBy(Node node, List<SortItem> sortItems, Scope orderByScope) {
    ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();

    for (SortItem item : sortItems) {
      Expression expression = item.getSortKey();

      ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, orderByScope);
//      analysis.recordSubqueries(node, expressionAnalysis);
      Type type = analysis.getType(expression);
//          .get();
      if (!type.isOrderable()) {
        throw new RuntimeException(String.format(
            "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s", type, expression));
      }

      orderByFieldsBuilder.add(expression);
    }

    List<Expression> orderByFields = orderByFieldsBuilder.build();
    return orderByFields;
  }

  public List<SortItem> getSortItemsFromOrderBy(Optional<OrderBy> orderBy) {
    return orderBy.map(OrderBy::getSortItems).orElse(ImmutableList.of());
  }

  private Scope createAndAssignScope(Node node, Scope parentScope, List<Field> fields) {
    return createAndAssignScope(node, parentScope, new RelationType<Field>(fields));
  }

  private Scope createAndAssignScope(Node node, Scope parentScope, RelationType relationType) {
    Scope scope = Scope.builder()
        .withParent(parentScope)
        .withRelationType(relationType)
        .build();

    analysis.setScope(node, scope);

    return scope;
  }

  private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope,
      QuerySpecification querySpecification) {
    // ORDER BY should "see" both output and FROM fields during initial analysis and non-aggregation query planning
    //Todo move to function
    Scope orderByScope = Scope.builder()
        .withParent(sourceScope)
        .withRelationType(outputScope.getRelation())
        .build();
    analysis.setScope(node, orderByScope);
    return orderByScope;
  }

  private Scope computeAndAssignOrderByScopeWithAggregation(OrderBy node, Scope sourceScope, Scope outputScope, List<FunctionCall> aggregations, List<Expression> groupByExpressions, List<GroupingOperation> groupingOperations)
  {
    //todo
//
//      // This scope is only used for planning. When aggregation is present then
//      // only output fields, groups and aggregation expressions should be visible from ORDER BY expression
//      ImmutableList.Builder<Expression> orderByAggregationExpressionsBuilder = ImmutableList.<Expression>builder()
//          .addAll(groupByExpressions)
//          .addAll(aggregations)
//          .addAll(groupingOperations);
//
//      // Don't add aggregate complex expressions that contains references to output column because the names would clash in TranslationMap during planning.
//      List<Expression> orderByExpressionsReferencingOutputScope = AstUtils.preOrder(node)
//          .filter(Expression.class::isInstance)
//          .map(Expression.class::cast)
//          .filter(expression -> hasReferencesToScope(expression, analysis, outputScope))
//          .collect(toImmutableList());
//      List<Expression> orderByAggregationExpressions = orderByAggregationExpressionsBuilder.build().stream()
//          .filter(expression -> !orderByExpressionsReferencingOutputScope.contains(expression) || analysis.isColumnReference(expression))
//          .collect(toImmutableList());
//
//      // generate placeholder fields
//      Set<Field> seen = new HashSet<>();
//      List<Field> orderByAggregationSourceFields = orderByAggregationExpressions.stream()
//          .map(expression -> {
//            // generate qualified placeholder field for GROUP BY expressions that are column references
//            Optional<Field> sourceField = sourceScope.tryResolveField(expression)
//                .filter(resolvedField -> seen.add(resolvedField.getField()))
//                .map(ResolvedField::getField);
//            return sourceField
//                .orElse(Field.newUnqualified(Optional.empty(), analysis.getType(expression).get()));
//          })
//          .collect(toImmutableList());
//
//      Scope orderByAggregationScope = Scope.builder()
//          .withRelationType(node, new RelationSqmlType(orderByAggregationSourceFields))
//          .build();
//
//      Scope orderByScope = Scope.builder()
//          .withParent(orderByAggregationScope)
//          .withRelationType(node, outputScope.getRelationType())
//          .build();
//      analysis.setScope(node, orderByScope);
//      analysis.setOrderByAggregates(node, orderByAggregationExpressions);
    return outputScope;
  }
  private Scope computeAndAssignOutputScope(QuerySpecification node, Scope scope,
      Scope sourceScope) {
    Builder<StandardField> outputFields = ImmutableList.builder();

    for (SelectItem item : node.getSelect().getSelectItems()) {
      if (item instanceof AllColumns) {
        Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

        for (Field field : sourceScope.resolveFieldsWithPrefix(starPrefix)) {
          outputFields.add(new StandardField(field.getName(), field.getType(), List.of(), Optional.empty()));
        }
      } else if (item instanceof SingleColumn) {
        SingleColumn column = (SingleColumn) item;

        Expression expression = column.getExpression();
        Optional<Identifier> field = column.getAlias();

        Optional<QualifiedObjectName> originTable = Optional.empty();
        Optional<String> originColumn = Optional.empty();
        QualifiedName name = null;

        if (expression instanceof Identifier) {
          name = QualifiedName.of(((Identifier) expression).getValue());
        }
        else if (expression instanceof DereferenceExpression) {
          name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
        }

        if (name != null) {
//            List<Field> matchingFields = sourceScope.resolveFields(name);
//            if (!matchingFields.isEmpty()) {
//              originTable = matchingFields.get(0).getOriginTable();
//              originColumn = matchingFields.get(0).getOriginColumnName();
//            }
        }

        if (field.isEmpty()) {
          if (name != null) {
            field = Optional.of(new Identifier(getLast(name.getOriginalParts())));
          }
        }

        String identifierName = field.map(Identifier::getValue)
            .orElse("VAR");

        if (analysis.getType(expression) == null) {
          log.error("analysis type could not be found:" + expression);
          continue;
        }
        outputFields.add(
            new StandardField(Name.of(identifierName, NameCanonicalizer.SYSTEM),
            analysis.getType(expression), List.of(), Optional.empty())
//            column.getAlias().isPresent())
        );
      }
      else {
        throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
      }
    }

    return createAndAssignScope(node, scope, new ArrayList<>(outputFields.build()));
  }

  private void analyzeHaving(QuerySpecification node, Scope scope) {
    if (node.getHaving().isPresent()) {
      Expression predicate = node.getHaving().get();

      ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);


//      analysis.recordSubqueries(node, expressionAnalysis);

      Type predicateType = expressionAnalysis.getType(predicate);
      if (!(predicateType instanceof BooleanType)) {
        throw new RuntimeException(String.format("HAVING clause must evaluate to a boolean: actual type %s", predicateType));
      }

      analysis.setHaving(node, predicate);
    }
  }

  private List<Expression> analyzeSelect(QuerySpecification node, Scope scope) {
    List<Expression> outputExpressions = new ArrayList<>();

    for (SelectItem item : node.getSelect().getSelectItems()) {
      if (item instanceof AllColumns) {
        Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

        List<TypedField> fields = scope.resolveFieldsWithPrefix(starPrefix);
        if (fields.isEmpty()) {
          if (starPrefix.isPresent()) {
            throw new RuntimeException(String.format("Table '%s' not found", starPrefix.get()));
          }
          throw new RuntimeException(String.format("SELECT * not allowed from relation that has no columns"));
        }
        for (Field field : fields) {
//            int fieldIndex = scope.getRelation().indexOf(field);
//            FieldReference expression = new FieldReference(fieldIndex);
//            outputExpressions.add(expression);
//            ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);

          if (node.getSelect().isDistinct() && !field.getType().isComparable()) {
            throw new RuntimeException(String.format("DISTINCT can only be applied to comparable types (actual: %s)", field.getType()));
          }
        }
      } else if (item instanceof SingleColumn) {
        SingleColumn column = (SingleColumn) item;
        ExpressionAnalysis expressionAnalysis = analyzeExpression(column.getExpression(), scope);

//        analysis.recordSubqueries(node, expressionAnalysis);
        outputExpressions.add(column.getExpression());

        Type type = expressionAnalysis.getType(column.getExpression());
        if (node.getSelect().isDistinct() && !type.isComparable()) {
          throw new RuntimeException(String.format("DISTINCT can only be applied to comparable types (actual: %s): %s", type, column.getExpression()));
        }
      }
      else {
        throw new IllegalArgumentException(String.format("Unsupported SelectItem type: %s", item.getClass().getName()));
      }
    }

    analysis.setOutputExpressions(node, outputExpressions);

    return outputExpressions;
  }

  public RelationType join(RelationType left, RelationType right) {
    List<Field> joinFields = new ArrayList<>();
    joinFields.addAll(left.getFields());
    joinFields.addAll(right.getFields());
    return new RelationType(joinFields);
  }

  private List<Expression> analyzeGroupBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions)
  {
    if (node.getGroupBy().isEmpty()) {
      return List.of();
    }

    List<Set<FieldId>> set = new ArrayList();
    List<Expression> groupingExpressions = new ArrayList();
    GroupingElement groupingElement = node.getGroupBy().get().getGroupingElement();
    for (Expression column : groupingElement.getExpressions()) {
      if (column instanceof LongLiteral) {
        throw new RuntimeException("Ordinals not supported in group by statements");
      }

      if (analysis.getColumnReferenceFields().containsKey(NodeRef.of(column))) {
        set.add(
            ImmutableSet.copyOf(analysis.getColumnReferenceFields().get(NodeRef.of(column))));
      }
//      else {
//        verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), metadata.getFunctionAndTypeManager(), column, "GROUP BY clause");
////        analysis.recordSubqueries(node, analyzeExpression(column, scope));
//        complexExpressions.add(column);
//      }
      //Group by statement must be one of the select fields
      if (!(column instanceof Identifier)) {
        log.info(String.format("GROUP BY statement should use column aliases instead of expressions. %s", column));
        analyzeExpression(column, scope);
        outputExpressions.stream()
            .filter(e->e.equals(column))
            .findAny()
            .orElseThrow(()->new RuntimeException(String.format("SELECT should contain GROUP BY expression %s", column)));
        groupingExpressions.add(column);
      } else {
        Expression rewrittenGroupByExpression = ExpressionTreeRewriter.rewriteWith(
            new OrderByExpressionRewriter(extractNamedOutputExpressions(node.getSelect())), column);
        int index = outputExpressions.indexOf(rewrittenGroupByExpression);
        if (index == -1) {
          throw new RuntimeException(String.format("SELECT should contain GROUP BY expression %s", column));
        }
        groupingExpressions.add(outputExpressions.get(index));
      }
    }

    for (Expression expression : groupingExpressions) {
      Type type = analysis.getType(expression);
//          .get();
      if (!type.isComparable()) {
        throw new RuntimeException(String.format("%s is not comparable, and therefore cannot be used in GROUP BY", type));
      }
    }

    analysis.setGroupByExpressions(node, groupingExpressions);
    analysis.setGroupingSets(node, set);

    return groupingExpressions;
  }

  private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
    ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata, planBuilder);
    ExpressionAnalysis exprAnalysis = analyzer.analyze(expression, scope);

    analysis.addCoercions(exprAnalysis.getExpressionCoercions(),
        exprAnalysis.getTypeOnlyCoercions());
    analysis.addTypes(exprAnalysis.getExpressionTypes());
    analysis.addFunctionHandles(exprAnalysis.getResolvedFunctions());
    analysis.addColumnReferences(exprAnalysis.getColumnReferences());
    return exprAnalysis;
  }
}
