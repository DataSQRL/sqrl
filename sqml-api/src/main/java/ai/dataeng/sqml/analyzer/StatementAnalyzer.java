package ai.dataeng.sqml.analyzer;

import static com.google.common.collect.Iterables.getLast;
import static java.util.Collections.emptyList;

import ai.dataeng.sqml.OperatorType.QualifiedObjectName;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.DereferenceExpression;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.GroupingElement;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.Statement;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class StatementAnalyzer {

  private final Metadata metadata;
  private final Analysis analysis;

  public StatementAnalyzer(Metadata metadata, Analysis analysis) {

    this.metadata = metadata;
    this.analysis = analysis;
  }

  public Scope analyze(Statement statement,
      Scope scope) {
    Visitor visitor = new Visitor();
    return statement.accept(visitor, scope);
  }

  public class Visitor extends AstVisitor<Scope, Scope> {

    @Override
    protected Scope visitNode(Node node, Scope context) {
      throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    //todo:
    // Order by & limit on Query node
    @Override
    protected Scope visitQuery(Query node, Scope scope)
    {
      Scope queryBodyScope = node.getQueryBody().accept(this, scope);

      // Input fields == Output fields
      analysis.setOutputExpressions(node, descriptorToFields(queryBodyScope));

      Scope queryScope = Scope.builder()
          .withParent(queryBodyScope)
          .withRelationType(node, queryBodyScope.getRelationType())
          .build();

      analysis.setScope(node, queryScope);
      return queryScope;
    }

    @Override
    protected Scope visitQuerySpecification(QuerySpecification node, Scope scope)
    {
      Scope sourceScope = analyzeFrom(node, scope);

      //node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

      List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
      List<Expression> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
//      analyzeHaving(node, sourceScope);
      Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

      List<Expression> orderByExpressions = emptyList();
      Optional<Scope> orderByScope = Optional.empty();
      if (node.getOrderBy().isPresent()) {
//        throw new RuntimeException("Order by tbd");
//        if (node.getSelect().isDistinct()) {
//          verifySelectDistinct(node, outputExpressions);
//        }
//
//        OrderBy orderBy = node.getOrderBy().get();
//        orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope));
//
//        orderByExpressions = analyzeOrderBy(node, orderBy.getSortItems(), orderByScope.get());
//
//        if (sourceScope.getOuterQueryParent().isPresent() && !node.getLimit().isPresent()) {
//          // not the root scope and ORDER BY is ineffective
//          analysis.markRedundantOrderBy(orderBy);
//          warningCollector.add(new PrestoWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
//        }
      }
//      analysis.setOrderByExpressions(node, orderByExpressions);
//
//      List<Expression> sourceExpressions = new ArrayList<>(outputExpressions);
//      node.getHaving().ifPresent(sourceExpressions::add);
//
//      analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
//      List<FunctionCall> aggregates = analyzeAggregations(node, sourceExpressions, orderByExpressions);
//
//      if (!aggregates.isEmpty() && groupByExpressions.isEmpty()) {
//        // Have Aggregation functions but no explicit GROUP BY clause
//        analysis.setGroupByExpressions(node, ImmutableList.of());
//      }
//
//      verifyAggregations(node, sourceScope, orderByScope, groupByExpressions, sourceExpressions, orderByExpressions);
//
//      analyzeWindowFunctions(node, outputExpressions, orderByExpressions);
//
//      if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
//        // Create a different scope for ORDER BY expressions when aggregation is present.
//        // This is because planner requires scope in order to resolve names against fields.
//        // Original ORDER BY scope "sees" FROM query fields. However, during planning
//        // and when aggregation is present, ORDER BY expressions should only be resolvable against
//        // output scope, group by expressions and aggregation expressions.
//        List<GroupingOperation> orderByGroupingOperations = extractExpressions(orderByExpressions, GroupingOperation.class);
//        List<FunctionCall> orderByAggregations = extractAggregateFunctions(analysis.getFunctionHandles(), orderByExpressions, metadata.getFunctionAndTypeManager());
//        computeAndAssignOrderByScopeWithAggregation(node.getOrderBy().get(), sourceScope, outputScope, orderByAggregations, groupByExpressions, orderByGroupingOperations);
//      }
      return outputScope;
    }

    @Override
    protected Scope visitTable(Table table, Scope scope) {
      QualifiedName name = dereferenceTableName(table.getName(), scope.getName());

      Optional<RelationSqmlType> tableHandle = scope.getRelation(name);
      if (tableHandle.isEmpty()) {
        //If no Source that emits that object can be identified, throw error
        throw new RuntimeException(String.format("Could not resolve table %s", table.getName()));
      }

      RelationSqmlType relation = tableHandle.get();

      //Get all defined fields
      List<Field> fields = relation.getFields();
      return createAndAssignScope(table, scope, fields);
    }

    @Override
    protected Scope visitJoin(Join node, Scope scope) {
      Scope left = node.getLeft().accept(this, scope);
      Scope right = node.getRight().accept(this, scope);

      JoinCriteria criteria = node.getCriteria().get();

      Scope result = createAndAssignScope(node, scope, left.getRelationType()
          .join(right.getRelationType()));

      if (criteria instanceof JoinOn) {
        Expression expression = ((JoinOn) criteria).getExpression();

        // need to register coercions in case when join criteria requires coercion (e.g. join on char(1) = char(2))
        ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, result);
        SqmlType clauseType = expressionAnalysis.getType(expression);
        if (!(clauseType instanceof BooleanSqmlType)) {
          throw new RuntimeException(String.format("JOIN ON clause must evaluate to a boolean: actual type %s", clauseType));
        }

        //todo: restrict grouping criteria
        analysis.setJoinCriteria(node, expression);
      }

      return result;
    }

    @Override
    protected Scope visitAliasedRelation(AliasedRelation relation, Scope scope) {
      Scope relationScope = relation.getRelation().accept(this, scope);

      RelationSqmlType relationType = relationScope.getRelationType();

      RelationSqmlType descriptor = relationType.withAlias(relation.getAlias().getValue());

      return createAndAssignScope(relation, scope, descriptor);
    }

    private List<Expression> descriptorToFields(Scope scope) {
      return null;
    }

    private Scope createScope(Scope scope) {
      return new Scope(scope.getName());
    }

    private Scope createAndAssignScope(Node node, Scope parentScope)
    {
      return createAndAssignScope(node, parentScope, new ArrayList<>());
    }

    private Scope createAndAssignScope(Node node, Scope parentScope, Field... fields)
    {
      return createAndAssignScope(node, parentScope, new RelationSqmlType(fields));
    }

    private Scope createAndAssignScope(Node node, Scope parentScope, List<Field> fields)
    {
      return createAndAssignScope(node, parentScope, new RelationSqmlType(fields));
    }

    private Scope createAndAssignScope(Node node, Scope parentScope, RelationSqmlType relationType)
    {
      Scope scope = scopeBuilder(parentScope)
          .withRelationType(node, relationType)
          .withName(parentScope.getName())
          .build();

      analysis.setScope(node, scope);
      return scope;
    }

    private Scope.Builder scopeBuilder(Scope parentScope)
    {
      Scope.Builder scopeBuilder = Scope.builder();
      scopeBuilder.withName(parentScope.getName());

      if (parentScope != null) {
        // parent scope represents local query scope hierarchy. Local query scope
        // hierarchy should have outer query scope as ancestor already.
        scopeBuilder.withParent(parentScope);
      }
//      else if (outerQueryScope.isPresent()) {
//        scopeBuilder.withOuterQueryParent(outerQueryScope.get());
//      }

      return scopeBuilder;
    }

    private QualifiedName dereferenceTableName(QualifiedName name,
        QualifiedName scopeName) {
      if (name.getParts().get(0).equalsIgnoreCase("@")) {
        List<String> newName = new ArrayList<>(scopeName.getParts().subList(0, scopeName.getParts().size() - 1));
        newName.addAll(name.getParts().subList(1, name.getParts().size()));
        return QualifiedName.of(newName);
      }

      return name;
    }

    private Scope computeAndAssignOutputScope(QuerySpecification node, Scope scope,
        Scope sourceScope)
    {
      Builder<Field> outputFields = ImmutableList.builder();

      for (SelectItem item : node.getSelect().getSelectItems()) {
        if (item instanceof AllColumns) {
          throw new RuntimeException("Select * tbd");
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
            List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
            if (!matchingFields.isEmpty()) {
              originTable = matchingFields.get(0).getOriginTable();
              originColumn = matchingFields.get(0).getOriginColumnName();
            }
          }

          if (field.isEmpty()) {
            if (name != null) {
              field = Optional.of(new Identifier(getLast(name.getOriginalParts())));
            }
          }

          outputFields.add(Field.newUnqualified(field.map(Identifier::getValue),
              analysis.getType(expression), originTable, originColumn,
              column.getAlias().isPresent()));
        }
        else {
          throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
        }
      }

      return createAndAssignScope(node, scope, outputFields.build());
    }
    private void analyzeHaving(QuerySpecification node, Scope sourceScope) {
      throw new RuntimeException("Having TBD");
    }

    private List<Expression> analyzeSelect(QuerySpecification node, Scope scope) {
      List<Expression> outputExpressions = new ArrayList<>();

      for (SelectItem item : node.getSelect().getSelectItems()) {
        if (item instanceof AllColumns) {
          throw new RuntimeException("Select * not yet implemented");
        } else if (item instanceof SingleColumn) {
          SingleColumn column = (SingleColumn) item;
          //creates expression to type mapping
          analyzeExpression(column.getExpression(), scope);
          analysis.addName(column.getExpression(), column.getAlias());
          //analysis.recordSubqueries(node, expressionAnalysis);
          outputExpressions.add(column.getExpression());

          //SqmlType type = expressionAnalysis.getType(column.getExpression());
          //if (node.getSelect().isDistinct() && !type.isComparable()) {
          //  throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s): %s", type, column.getExpression());
          //}
        }
        else {
          throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
        }
      }

      analysis.setOutputExpressions(node, outputExpressions);

      return outputExpressions;
    }


    private List<Expression> analyzeGroupBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions)
    {
      if (node.getGroupBy().isPresent()) {
        List<List<Set<Object>>> sets = new ArrayList();
        //List<Expression> complexExpressions = new ArrayList();
        List<Expression> groupingExpressions = new ArrayList();

        for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
          for (Expression column : groupingElement.getExpressions()) {

            analyzeExpression(column, scope);

            if (analysis.getColumnReferences().containsKey(NodeRef.of(column))) {
              sets.add(ImmutableList.of(ImmutableSet.copyOf(analysis.getColumnReferences().get(NodeRef.of(column)))));
            } else {
              throw new RuntimeException("TBD complex group by expressions");
              //verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(), metadata.getFunctionAndTypeManager(), column, "GROUP BY clause");
//              analysis.recordSubqueries(node, analyzeExpression(column, scope));
//              complexExpressions.add(column);
            }

            groupingExpressions.add(column);
          }
        }

        analysis.setGroupByExpressions(node, groupingExpressions);
        analysis.setGroupingSets(node, sets);

        return groupingExpressions;
      }

      return ImmutableList.of();
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
      ExpressionAnalysis exprAnalysis = analyzer.analyze(expression, scope);

      analysis.addTypes(exprAnalysis.getExpressionTypes());
//      analysis.addCoercions(expressionCoercions, typeOnlyCoercions);
//      analysis.addFunctionHandles(resolvedFunctions);
      analysis.addColumnReferences(exprAnalysis.getColumnReferences());
//      analysis.addLambdaArgumentReferences(analyzer.getLambdaArgumentReferences());
//      analysis.addTableColumnReferences(accessControl, session.getIdentity(), analyzer.getTableColumnReferences());

      return exprAnalysis;
    }

    private Scope analyzeFrom(QuerySpecification node, Scope scope)
    {
      if (node.getFrom().isPresent()) {
        return node.getFrom().get().accept(this, scope);
      }

      return createScope(scope);
    }
  }
}
