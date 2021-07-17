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

import static ai.dataeng.sqml.analyzer.AggregationAnalyzer.verifyOrderByAggregations;
import static ai.dataeng.sqml.analyzer.AggregationAnalyzer.verifySourceAggregations;
import static ai.dataeng.sqml.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static ai.dataeng.sqml.analyzer.ExpressionTreeUtils.extractExpressions;
import static ai.dataeng.sqml.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.MISSING_TABLE;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.TOO_MANY_GROUPING_SETS;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.WILDCARD_WITHOUT_FROM;
import static ai.dataeng.sqml.common.type.BooleanType.BOOLEAN;
import static ai.dataeng.sqml.common.type.UnknownType.UNKNOWN;
import static ai.dataeng.sqml.metadata.MetadataUtil.createQualifiedObjectName;
import static ai.dataeng.sqml.planner.ExpressionDeterminismEvaluator.isDeterministic;
import static ai.dataeng.sqml.planner.NodeUtils.getSortItemsFromOrderBy;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.common.CatalogSchemaName;
import ai.dataeng.sqml.common.ColumnHandle;
import ai.dataeng.sqml.common.QualifiedObjectName;
import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.function.FunctionHandle;
import ai.dataeng.sqml.metadata.ColumnMetadata;
import ai.dataeng.sqml.metadata.FunctionAndTypeManager;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.metadata.TableMetadata;
import ai.dataeng.sqml.relation.TableHandle;
import ai.dataeng.sqml.sql.parser.SqlParser;
import ai.dataeng.sqml.sql.tree.AliasedRelation;
import ai.dataeng.sqml.sql.tree.AllColumns;
import ai.dataeng.sqml.sql.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.sql.tree.DereferenceExpression;
import ai.dataeng.sqml.sql.tree.Except;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.ExpressionRewriter;
import ai.dataeng.sqml.sql.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.sql.tree.FieldReference;
import ai.dataeng.sqml.sql.tree.FunctionCall;
import ai.dataeng.sqml.sql.tree.GroupBy;
import ai.dataeng.sqml.sql.tree.GroupingElement;
import ai.dataeng.sqml.sql.tree.GroupingOperation;
import ai.dataeng.sqml.sql.tree.GroupingSets;
import ai.dataeng.sqml.sql.tree.Identifier;
import ai.dataeng.sqml.sql.tree.Intersect;
import ai.dataeng.sqml.sql.tree.Join;
import ai.dataeng.sqml.sql.tree.JoinCriteria;
import ai.dataeng.sqml.sql.tree.JoinOn;
import ai.dataeng.sqml.sql.tree.LongLiteral;
import ai.dataeng.sqml.sql.tree.NaturalJoin;
import ai.dataeng.sqml.sql.tree.Node;
import ai.dataeng.sqml.sql.tree.NodeRef;
import ai.dataeng.sqml.sql.tree.OrderBy;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import ai.dataeng.sqml.sql.tree.Query;
import ai.dataeng.sqml.sql.tree.QuerySpecification;
import ai.dataeng.sqml.sql.tree.Relation;
import ai.dataeng.sqml.sql.tree.Select;
import ai.dataeng.sqml.sql.tree.SelectItem;
import ai.dataeng.sqml.sql.tree.SetOperation;
import ai.dataeng.sqml.sql.tree.SimpleGroupBy;
import ai.dataeng.sqml.sql.tree.SingleColumn;
import ai.dataeng.sqml.sql.tree.SortItem;
import ai.dataeng.sqml.sql.tree.Table;
import ai.dataeng.sqml.sql.tree.TableSubquery;
import ai.dataeng.sqml.sql.tree.Union;
import ai.dataeng.sqml.sql.util.AstUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StatementAnalyzer {
  private final Analysis analysis;
  private final Metadata metadata;
  private final Session session;
  private final SqlParser sqlParser;

  public StatementAnalyzer(
      Analysis analysis,
      Metadata metadata,
      SqlParser sqlParser,
      Session session) {
    this.analysis = requireNonNull(analysis, "analysis is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    this.session = requireNonNull(session, "session is null");
  }

  private static boolean hasScopeAsLocalParent(Scope root, Scope parent) {
    Scope scope = root;
    while (scope.getLocalParent().isPresent()) {
      scope = scope.getLocalParent().get();
      if (scope.equals(parent)) {
        return true;
      }
    }

    return false;
  }

  public Scope analyze(Node node, Scope outerQueryScope) {
    return analyze(node, Optional.of(outerQueryScope));
  }

  public Scope analyze(Node node, Optional<Scope> outerQueryScope) {
    return new Visitor(outerQueryScope).process(node, Optional.empty());
  }

  /**
   * Visitor context represents local query scope (if exists). The invariant is that the local query
   * scopes hierarchy should always have outer query scope (if provided) as ancestor.
   */
  private class Visitor
      extends DefaultTraversalVisitor<Scope, Optional<Scope>> {

    private final Optional<Scope> outerQueryScope;

    private Visitor(Optional<Scope> outerQueryScope) {
      this.outerQueryScope = requireNonNull(outerQueryScope, "outerQueryScope is null");
    }

    public Scope process(Node node, Optional<Scope> scope) {
      Scope returnScope = super.process(node, scope);
      checkState(returnScope.getOuterQueryParent().equals(outerQueryScope),
          "result scope should have outer query scope equal with parameter outer query scope");
      if (scope.isPresent()) {
        checkState(hasScopeAsLocalParent(returnScope, scope.get()),
            "return scope should have context scope as one of ancestors");
      }
      return returnScope;
    }

    private Scope process(Node node, Scope scope) {
      return process(node, Optional.of(scope));
    }

    @Override
    protected Scope visitQuery(Query node, Optional<Scope> scope) {
      Scope withScope = createScope(scope);
      Scope queryBodyScope = process(node.getQueryBody(), withScope);
      List<Expression> orderByExpressions = emptyList();
      if (node.getOrderBy().isPresent()) {
        orderByExpressions = analyzeOrderBy(node, getSortItemsFromOrderBy(node.getOrderBy()),
            queryBodyScope);
        if (queryBodyScope.getOuterQueryParent().isPresent() && !node.getLimit().isPresent()) {
          // not the root scope and ORDER BY is ineffective
          analysis.markRedundantOrderBy(node.getOrderBy().get());
        }
      }
      analysis.setOrderByExpressions(node, orderByExpressions);

      // Input fields == Output fields
      analysis.setOutputExpressions(node, descriptorToFields(queryBodyScope));

      Scope queryScope = Scope.builder()
          .withParent(withScope)
          .withRelationType(RelationId.of(node), queryBodyScope.getRelationType())
          .build();

      analysis.setScope(node, queryScope);
      return queryScope;
    }

    @Override
    protected Scope visitTable(Table table, Optional<Scope> scope) {
      QualifiedObjectName name = createQualifiedObjectName(session, table, table.getName());
      if (name.getObjectName().isEmpty()) {
        throw new SemanticException(MISSING_TABLE, table, "Table name is empty");
      }
      if (name.getSchemaName().isEmpty()) {
        throw new SemanticException(MISSING_SCHEMA, table, "Schema name is empty");
      }

      Optional<TableHandle> tableHandle = metadata.getRelationHandle(session, name);
      if (!tableHandle.isPresent()) {
        if (!metadata.getCatalogHandle(session, name.getCatalogName()).isPresent()) {
          throw new SemanticException(MISSING_CATALOG, table, "Catalog %s does not exist",
              name.getCatalogName());
        }
        if (!metadata.schemaExists(session,
            new CatalogSchemaName(name.getCatalogName(), name.getSchemaName()))) {
          throw new SemanticException(MISSING_SCHEMA, table, "Schema %s does not exist",
              name.getSchemaName());
        }
        throw new SemanticException(MISSING_TABLE, table, "Table %s does not exist", name);
      }

      TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());

      ImmutableList.Builder<Field> fields = ImmutableList.builder();
      for (ColumnMetadata column : tableMetadata.getColumns()) {
        Field field = Field.newQualified(
            table.getName(),
            Optional.of(column.getName()),
            column.getType(),
            column.isHidden(),
            Optional.of(name),
            Optional.of(column.getName()),
            false);
        fields.add(field);
        ColumnHandle columnHandle = metadata
            .getColumnHandle(session, column.getName());
        checkArgument(columnHandle != null, "Unknown field %s", field);
        analysis.setColumn(field, columnHandle);
      }

      analysis.registerTable(table, tableHandle.get());
      return createAndAssignScope(table, scope, fields.build());
    }

    @Override
    protected Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope) {
      Scope relationScope = process(relation.getRelation(), scope);

      // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the node object
      RelationType relationType = relationScope.getRelationType();
      if (relation.getColumnNames() != null) {
        int totalColumns = relationType.getVisibleFieldCount();
        if (totalColumns != relation.getColumnNames().size()) {
          throw new SemanticException(MISMATCHED_COLUMN_ALIASES, relation,
              "Column alias list has %s entries but '%s' has %s columns available",
              relation.getColumnNames().size(), relation.getAlias(), totalColumns);
        }
      }

      List<String> aliases = null;
      if (relation.getColumnNames() != null) {
        aliases = relation.getColumnNames().stream()
            .map(Identifier::getValue)
            .collect(Collectors.toList());
      }

      RelationType descriptor = relationType.withAlias(relation.getAlias().getValue(), aliases);

      return createAndAssignScope(relation, scope, descriptor);
    }

    @Override
    protected Scope visitTableSubquery(TableSubquery node, Optional<Scope> scope) {
      StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser,
          session);
      Scope queryScope = analyzer.analyze(node.getQuery(), scope);
      return createAndAssignScope(node, scope, queryScope.getRelationType());
    }

    @Override
    protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope) {
      // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
      // to pass down to analyzeFrom

      Scope sourceScope = analyzeFrom(node, scope);

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
        orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope));

        orderByExpressions = analyzeOrderBy(node, orderBy.getSortItems(), orderByScope.get());

        if (sourceScope.getOuterQueryParent().isPresent() && !node.getLimit().isPresent()) {
          // not the root scope and ORDER BY is ineffective
          analysis.markRedundantOrderBy(orderBy);
        }
      }
      analysis.setOrderByExpressions(node, orderByExpressions);

      List<Expression> sourceExpressions = new ArrayList<>(outputExpressions);
      node.getHaving().ifPresent(sourceExpressions::add);

      analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
      List<FunctionCall> aggregates = analyzeAggregations(node, sourceExpressions,
          orderByExpressions);

      if (!aggregates.isEmpty() && groupByExpressions.isEmpty()) {
        // Have Aggregation functions but no explicit GROUP BY clause
        analysis.setGroupByExpressions(node, ImmutableList.of());
      }

      verifyAggregations(node, sourceScope, orderByScope, groupByExpressions, sourceExpressions,
          orderByExpressions);

      if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
        // Create a different scope for ORDER BY expressions when aggregation is present.
        // This is because planner requires scope in order to resolve names against fields.
        // Original ORDER BY scope "sees" FROM query fields. However, during planning
        // and when aggregation is present, ORDER BY expressions should only be resolvable against
        // output scope, group by expressions and aggregation expressions.
        List<GroupingOperation> orderByGroupingOperations = extractExpressions(orderByExpressions,
            GroupingOperation.class);
        List<FunctionCall> orderByAggregations = extractAggregateFunctions(
            analysis.getFunctionHandles(), orderByExpressions,
            metadata.getFunctionAndTypeManager());
        computeAndAssignOrderByScopeWithAggregation(node.getOrderBy().get(), sourceScope,
            outputScope, orderByAggregations, groupByExpressions, orderByGroupingOperations);
      }

      return outputScope;
    }

    @Override
    protected Scope visitSetOperation(SetOperation node, Optional<Scope> scope) {
      checkState(node.getRelations().size() >= 2);
      List<Scope> relationScopes = node.getRelations().stream()
          .map(relation -> {
            Scope relationScope = process(relation, scope);
            return createAndAssignScope(relation, scope,
                relationScope.getRelationType().withOnlyVisibleFields());
          })
          .collect(toImmutableList());

      Type[] outputFieldTypes = relationScopes.get(0).getRelationType().getVisibleFields().stream()
          .map(Field::getType)
          .toArray(Type[]::new);
      int outputFieldSize = outputFieldTypes.length;
      for (Scope relationScope : relationScopes) {
        RelationType relationType = relationScope.getRelationType();
        int descFieldSize = relationType.getVisibleFields().size();
        String setOperationName = node.getClass().getSimpleName().toUpperCase(ENGLISH);
        if (outputFieldSize != descFieldSize) {
          throw new SemanticException(
              MISMATCHED_SET_COLUMN_TYPES,
              node,
              "%s query has different number of fields: %d, %d",
              setOperationName,
              outputFieldSize,
              descFieldSize);
        }
        for (int i = 0; i < descFieldSize; i++) {
          Type descFieldType = relationType.getFieldByIndex(i).getType();
          Optional<Type> commonSuperType = metadata.getFunctionAndTypeManager()
              .getCommonSuperType(outputFieldTypes[i], descFieldType);
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
        }
      }

      Field[] outputDescriptorFields = new Field[outputFieldTypes.length];
      RelationType firstDescriptor = relationScopes.get(0).getRelationType()
          .withOnlyVisibleFields();
      for (int i = 0; i < outputFieldTypes.length; i++) {
        Field oldField = firstDescriptor.getFieldByIndex(i);
        outputDescriptorFields[i] = new Field(
            oldField.getRelationAlias(),
            oldField.getName(),
            outputFieldTypes[i],
            oldField.isHidden(),
            oldField.getOriginTable(),
            oldField.getOriginColumnName(),
            oldField.isAliased());
      }

      for (int i = 0; i < node.getRelations().size(); i++) {
        Relation relation = node.getRelations().get(i);
        Scope relationScope = relationScopes.get(i);
        RelationType relationType = relationScope.getRelationType();
        for (int j = 0; j < relationType.getVisibleFields().size(); j++) {
          Type outputFieldType = outputFieldTypes[j];
          Type descFieldType = relationType.getFieldByIndex(j).getType();
          if (!outputFieldType.equals(descFieldType)) {
            analysis.addRelationCoercion(relation, outputFieldTypes);
            break;
          }
        }
      }
      return createAndAssignScope(node, scope, outputDescriptorFields);
    }

    @Override
    protected Scope visitUnion(Union node, Optional<Scope> scope) {
      return visitSetOperation(node, scope);
    }

    @Override
    protected Scope visitIntersect(Intersect node, Optional<Scope> scope) {
      if (!node.isDistinct().orElse(true)) {
        throw new SemanticException(NOT_SUPPORTED, node, "INTERSECT ALL not yet implemented");
      }

      return visitSetOperation(node, scope);
    }

    @Override
    protected Scope visitExcept(Except node, Optional<Scope> scope) {
      if (!node.isDistinct().orElse(true)) {
        throw new SemanticException(NOT_SUPPORTED, node, "EXCEPT ALL not yet implemented");
      }

      return visitSetOperation(node, scope);
    }

    @Override
    protected Scope visitJoin(Join node, Optional<Scope> scope) {
      JoinCriteria criteria = node.getCriteria().orElse(null);
      if (criteria instanceof NaturalJoin) {
        throw new SemanticException(NOT_SUPPORTED, node, "Natural join not supported");
      }

      Scope left = process(node.getLeft(), scope);
      Scope right = process(node.getRight(), scope);

      Scope output = createAndAssignScope(node, scope,
          left.getRelationType().joinWith(right.getRelationType()));

      if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT) {
        return output;
      } else if (criteria instanceof JoinOn) {
        Expression expression = ((JoinOn) criteria).getExpression();

        // need to register coercions in case when join criteria requires coercion (e.g. join on char(1) = char(2))
        ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, output);
        Type clauseType = expressionAnalysis.getType(expression);
        if (!clauseType.equals(BOOLEAN)) {
          if (!clauseType.equals(UNKNOWN)) {
            throw new SemanticException(TYPE_MISMATCH, expression,
                "JOIN ON clause must evaluate to a boolean: actual type %s", clauseType);
          }
          // coerce null to boolean
          analysis.addCoercion(expression, BOOLEAN, false);
        }

        verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(),
            metadata.getFunctionAndTypeManager(), expression, "JOIN clause");

        analysis.recordSubqueries(node, expressionAnalysis);
        analysis.setJoinCriteria(node, expression);
      } else {
        throw new UnsupportedOperationException(
            "unsupported join criteria: " + criteria.getClass().getName());
      }

      return output;
    }

    private void analyzeHaving(QuerySpecification node, Scope scope) {
      if (node.getHaving().isPresent()) {
        Expression predicate = node.getHaving().get();

        ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);

        analysis.recordSubqueries(node, expressionAnalysis);

        Type predicateType = expressionAnalysis.getType(predicate);
        if (!predicateType.equals(BOOLEAN) && !predicateType.equals(UNKNOWN)) {
          throw new SemanticException(TYPE_MISMATCH, predicate,
              "HAVING clause must evaluate to a boolean: actual type %s", predicateType);
        }

        analysis.setHaving(node, predicate);
      }
    }

    private Multimap<QualifiedName, Expression> extractNamedOutputExpressions(Select node) {
      // Compute aliased output terms so we can resolve order by expressions against them first
      ImmutableMultimap.Builder<QualifiedName, Expression> assignments = ImmutableMultimap
          .builder();
      for (SelectItem item : node.getSelectItems()) {
        if (item instanceof SingleColumn) {
          SingleColumn column = (SingleColumn) item;
          Optional<Identifier> alias = column.getAlias();
          if (alias.isPresent()) {
            assignments.put(QualifiedName.of(alias.get().getValue()),
                column.getExpression()); // TODO: need to know if alias was quoted
          } else if (column.getExpression() instanceof Identifier) {
            assignments.put(QualifiedName.of(((Identifier) column.getExpression()).getValue()),
                column.getExpression());
          }
        }
      }

      return assignments.build();
    }

    private void checkGroupingSetsCount(GroupBy node) {
      // If groupBy is distinct then crossProduct will be overestimated if there are duplicate grouping sets.
      int crossProduct = 1;
      for (GroupingElement element : node.getGroupingElements()) {
        try {
          int product;
          if (element instanceof SimpleGroupBy) {
            product = 1;
          } else if (element instanceof GroupingSets) {
            product = ((GroupingSets) element).getSets().size();
          } else {
            throw new UnsupportedOperationException(
                "Unsupported grouping element type: " + element.getClass().getName());
          }
          crossProduct = Math.multiplyExact(crossProduct, product);
        } catch (ArithmeticException e) {
          throw new SemanticException(TOO_MANY_GROUPING_SETS, node,
              "GROUP BY has more than %s grouping sets but can contain at most %s",
              Integer.MAX_VALUE, getMaxGroupingSets(session));
        }
        if (crossProduct > getMaxGroupingSets(session)) {
          throw new SemanticException(TOO_MANY_GROUPING_SETS, node,
              "GROUP BY has %s grouping sets but can contain at most %s", crossProduct,
              getMaxGroupingSets(session));
        }
      }
    }

    private int getMaxGroupingSets(Session session) {
      return 2048;
    }

    private List<Expression> analyzeGroupBy(QuerySpecification node, Scope scope,
        List<Expression> outputExpressions) {
      if (node.getGroupBy().isPresent()) {
        ImmutableList.Builder<Set<FieldId>> cubes = ImmutableList.builder();
        ImmutableList.Builder<List<FieldId>> rollups = ImmutableList.builder();
        ImmutableList.Builder<List<Set<FieldId>>> sets = ImmutableList.builder();
        ImmutableList.Builder<Expression> complexExpressions = ImmutableList.builder();
        ImmutableList.Builder<Expression> groupingExpressions = ImmutableList.builder();

        checkGroupingSetsCount(node.getGroupBy().get());
        for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
          if (groupingElement instanceof SimpleGroupBy) {
            for (Expression column : groupingElement.getExpressions()) {
              // simple GROUP BY expressions allow ordinals or arbitrary expressions
              if (column instanceof LongLiteral) {
                long ordinal = ((LongLiteral) column).getValue();
                if (ordinal < 1 || ordinal > outputExpressions.size()) {
                  throw new SemanticException(INVALID_ORDINAL, column,
                      "GROUP BY position %s is not in select list", ordinal);
                }

                column = outputExpressions.get(toIntExact(ordinal - 1));
              } else {
                analyzeExpression(column, scope);
              }

              if (analysis.getColumnReferenceFields().containsKey(NodeRef.of(column))) {
                sets.add(ImmutableList.of(ImmutableSet
                    .copyOf(analysis.getColumnReferenceFields().get(NodeRef.of(column)))));
              } else {
                verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(),
                    metadata.getFunctionAndTypeManager(), column, "GROUP BY clause");
                analysis.recordSubqueries(node, analyzeExpression(column, scope));
                complexExpressions.add(column);
              }

              groupingExpressions.add(column);
            }
          } else {
            for (Expression column : groupingElement.getExpressions()) {
              analyzeExpression(column, scope);
              if (!analysis.getColumnReferences().contains(NodeRef.of(column))) {
                throw new SemanticException(SemanticErrorCode.MUST_BE_COLUMN_REFERENCE, column,
                    "GROUP BY expression must be a column reference: %s", column);
              }

              groupingExpressions.add(column);
            }

            if (groupingElement instanceof GroupingSets) {
              List<Set<FieldId>> groupingSets = ((GroupingSets) groupingElement).getSets().stream()
                  .map(set -> set.stream()
                      .map(NodeRef::of)
                      .map(analysis.getColumnReferenceFields()::get)
                      .flatMap(Collection::stream)
                      .collect(toImmutableSet()))
                  .collect(toImmutableList());

              sets.add(groupingSets);
            }
          }
        }

        List<Expression> expressions = groupingExpressions.build();
        for (Expression expression : expressions) {
          Type type = analysis.getType(expression);
          if (!type.isComparable()) {
            throw new SemanticException(TYPE_MISMATCH, node,
                "%s is not comparable, and therefore cannot be used in GROUP BY", type);
          }
        }

        analysis.setGroupByExpressions(node, expressions);
        analysis.setGroupingSets(node,
            new Analysis.GroupingSetAnalysis(cubes.build(), rollups.build(), sets.build(),
                complexExpressions.build()));

        return expressions;
      }

      return ImmutableList.of();
    }

    private Scope computeAndAssignOutputScope(QuerySpecification node, Optional<Scope> scope,
        Scope sourceScope) {
      ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

      for (SelectItem item : node.getSelect().getSelectItems()) {
        if (item instanceof AllColumns) {
          // expand * and T.*
          Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

          for (Field field : sourceScope.getRelationType().resolveFieldsWithPrefix(starPrefix)) {
            outputFields.add(Field
                .newUnqualified(field.getName(), field.getType(), field.getOriginTable(),
                    field.getOriginColumnName(), false));
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
          } else if (expression instanceof DereferenceExpression) {
            name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
          }

          if (name != null) {
            List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
            if (!matchingFields.isEmpty()) {
              originTable = matchingFields.get(0).getOriginTable();
              originColumn = matchingFields.get(0).getOriginColumnName();
            }
          }

          if (!field.isPresent()) {
            if (name != null) {
              field = Optional.of(new Identifier(getLast(name.getOriginalParts())));
            }
          }

          outputFields.add(Field
              .newUnqualified(field.map(Identifier::getValue), analysis.getType(expression),
                  originTable, originColumn, column.getAlias()
                      .isPresent())); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
        } else {
          throw new IllegalArgumentException(
              "Unsupported SelectItem type: " + item.getClass().getName());
        }
      }

      return createAndAssignScope(node, scope, outputFields.build());
    }

    private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope) {
      // ORDER BY should "see" both output and FROM fields during initial analysis and non-aggregation query planning
      Scope orderByScope = Scope.builder()
          .withParent(sourceScope)
          .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
          .build();
      analysis.setScope(node, orderByScope);
      return orderByScope;
    }

    private Scope computeAndAssignOrderByScopeWithAggregation(OrderBy node, Scope sourceScope,
        Scope outputScope, List<FunctionCall> aggregations, List<Expression> groupByExpressions,
        List<GroupingOperation> groupingOperations) {
      // This scope is only used for planning. When aggregation is present then
      // only output fields, groups and aggregation expressions should be visible from ORDER BY expression
      ImmutableList.Builder<Expression> orderByAggregationExpressionsBuilder = ImmutableList
          .<Expression>builder()
          .addAll(groupByExpressions)
          .addAll(aggregations)
          .addAll(groupingOperations);

      // Don't add aggregate complex expressions that contains references to output column because the names would clash in TranslationMap during planning.
      List<Expression> orderByExpressionsReferencingOutputScope = AstUtils.preOrder(node)
          .filter(Expression.class::isInstance)
          .map(Expression.class::cast)
          .filter(expression -> hasReferencesToScope(expression, analysis, outputScope))
          .collect(toImmutableList());
      List<Expression> orderByAggregationExpressions = orderByAggregationExpressionsBuilder.build()
          .stream()
          .filter(expression -> !orderByExpressionsReferencingOutputScope.contains(expression)
              || analysis.isColumnReference(expression))
          .collect(toImmutableList());

      // generate placeholder fields
      Set<Field> seen = new HashSet<>();
      List<Field> orderByAggregationSourceFields = orderByAggregationExpressions.stream()
          .map(expression -> {
            // generate qualified placeholder field for GROUP BY expressions that are column references
            Optional<Field> sourceField = sourceScope.tryResolveField(expression)
                .filter(resolvedField -> seen.add(resolvedField.getField()))
                .map(ResolvedField::getField);
            return sourceField
                .orElse(Field.newUnqualified(Optional.empty(), analysis.getType(expression)));
          })
          .collect(toImmutableList());

      Scope orderByAggregationScope = Scope.builder()
          .withRelationType(RelationId.anonymous(),
              new RelationType(orderByAggregationSourceFields))
          .build();

      Scope orderByScope = Scope.builder()
          .withParent(orderByAggregationScope)
          .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
          .build();
      analysis.setScope(node, orderByScope);
      analysis.setOrderByAggregates(node, orderByAggregationExpressions);
      return orderByScope;
    }

    private List<Expression> analyzeSelect(QuerySpecification node, Scope scope) {
      ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();

      for (SelectItem item : node.getSelect().getSelectItems()) {
        if (item instanceof AllColumns) {
          // expand * and T.*
          Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

          RelationType relationType = scope.getRelationType();
          List<Field> fields = relationType.resolveFieldsWithPrefix(starPrefix);
          if (fields.isEmpty()) {
            if (starPrefix.isPresent()) {
              throw new SemanticException(MISSING_TABLE, item, "Table '%s' not found",
                  starPrefix.get());
            }
            if (!node.getFrom().isPresent()) {
              throw new SemanticException(WILDCARD_WITHOUT_FROM, item,
                  "SELECT * not allowed in queries without FROM clause");
            }
            throw new SemanticException(COLUMN_NAME_NOT_SPECIFIED, item,
                "SELECT * not allowed from relation that has no columns");
          }

          for (Field field : fields) {
            int fieldIndex = relationType.indexOf(field);
            FieldReference expression = new FieldReference(fieldIndex);
            outputExpressionBuilder.add(expression);
            ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);

            Type type = expressionAnalysis.getType(expression);
            if (node.getSelect().isDistinct() && !type.isComparable()) {
              throw new SemanticException(TYPE_MISMATCH, node.getSelect(),
                  "DISTINCT can only be applied to comparable types (actual: %s)", type);
            }
          }
        } else if (item instanceof SingleColumn) {
          SingleColumn column = (SingleColumn) item;
          ExpressionAnalysis expressionAnalysis = analyzeExpression(column.getExpression(), scope);
          analysis.recordSubqueries(node, expressionAnalysis);
          outputExpressionBuilder.add(column.getExpression());

          Type type = expressionAnalysis.getType(column.getExpression());
          if (node.getSelect().isDistinct() && !type.isComparable()) {
            throw new SemanticException(TYPE_MISMATCH, node.getSelect(),
                "DISTINCT can only be applied to comparable types (actual: %s): %s", type,
                column.getExpression());
          }
        } else {
          throw new IllegalArgumentException(
              "Unsupported SelectItem type: " + item.getClass().getName());
        }
      }

      ImmutableList<Expression> result = outputExpressionBuilder.build();
      analysis.setOutputExpressions(node, result);

      return result;
    }

    public void analyzeWhere(Node node, Scope scope, Expression predicate) {
      ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);

      verifyNoAggregateWindowOrGroupingFunctions(analysis.getFunctionHandles(),
          metadata.getFunctionAndTypeManager(), predicate, "WHERE clause");

      analysis.recordSubqueries(node, expressionAnalysis);

      Type predicateType = expressionAnalysis.getType(predicate);
      if (!predicateType.equals(BOOLEAN)) {
        if (!predicateType.equals(UNKNOWN)) {
          throw new SemanticException(TYPE_MISMATCH, predicate,
              "WHERE clause must evaluate to a boolean: actual type %s", predicateType);
        }
        // coerce null to boolean
        analysis.addCoercion(predicate, BOOLEAN, false);
      }

      analysis.setWhere(node, predicate);
    }

    private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope) {
      if (node.getFrom().isPresent()) {
        return process(node.getFrom().get(), scope);
      }

      return createScope(scope);
    }

    private void analyzeGroupingOperations(QuerySpecification node,
        List<Expression> outputExpressions, List<Expression> orderByExpressions) {
      List<GroupingOperation> groupingOperations = extractExpressions(
          Iterables.concat(outputExpressions, orderByExpressions), GroupingOperation.class);
      boolean isGroupingOperationPresent = !groupingOperations.isEmpty();

      if (isGroupingOperationPresent && !node.getGroupBy().isPresent()) {
        throw new SemanticException(
            INVALID_PROCEDURE_ARGUMENTS,
            node,
            "A GROUPING() operation can only be used with a corresponding GROUPING SET/CUBE/ROLLUP/GROUP BY clause");
      }

      analysis.setGroupingOperations(node, groupingOperations);
    }

    private List<FunctionCall> analyzeAggregations(
        QuerySpecification node,
        List<Expression> outputExpressions,
        List<Expression> orderByExpressions) {
      List<FunctionCall> aggregates = extractAggregateFunctions(analysis.getFunctionHandles(),
          Iterables.concat(outputExpressions, orderByExpressions),
          metadata.getFunctionAndTypeManager());
      analysis.setAggregates(node, aggregates);
      return aggregates;
    }

    private void verifyAggregations(
        QuerySpecification node,
        Scope sourceScope,
        Optional<Scope> orderByScope,
        List<Expression> groupByExpressions,
        List<Expression> outputExpressions,
        List<Expression> orderByExpressions) {
      checkState(orderByExpressions.isEmpty() || orderByScope.isPresent(),
          "non-empty orderByExpressions list without orderByScope provided");

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
          verifySourceAggregations(distinctGroupingColumns, sourceScope, expression, metadata,
              analysis);
        }

        for (Expression expression : orderByExpressions) {
          verifyOrderByAggregations(distinctGroupingColumns, sourceScope, orderByScope.get(),
              expression, metadata, analysis);
        }
      }
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      return ExpressionAnalyzer.analyzeExpression(
          session,
          metadata,
          sqlParser,
          scope,
          analysis,
          expression);
    }

    private List<Expression> descriptorToFields(Scope scope) {
      ImmutableList.Builder<Expression> builder = ImmutableList.builder();
      for (int fieldIndex = 0; fieldIndex < scope.getRelationType().getAllFieldCount();
          fieldIndex++) {
        FieldReference expression = new FieldReference(fieldIndex);
        builder.add(expression);
        analyzeExpression(expression, scope);
      }
      return builder.build();
    }

    private void verifySelectDistinct(QuerySpecification node, List<Expression> outputExpressions) {
      for (SortItem item : node.getOrderBy().get().getSortItems()) {
        Expression expression = item.getSortKey();

        if (expression instanceof LongLiteral) {
          continue;
        }

        Expression rewrittenOrderByExpression = ExpressionTreeRewriter.rewriteWith(
            new OrderByExpressionRewriter(extractNamedOutputExpressions(node.getSelect())),
            expression);
        int index = outputExpressions.indexOf(rewrittenOrderByExpression);
        if (index == -1) {
          throw new SemanticException(ORDER_BY_MUST_BE_IN_SELECT, node.getSelect(),
              "For SELECT DISTINCT, ORDER BY expressions must appear in select list");
        }

        if (!isDeterministic(expression)) {
          throw new SemanticException(NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT,
              expression,
              "Non deterministic ORDER BY expression is not supported with SELECT DISTINCT");
        }
      }
    }

    private List<Expression> analyzeOrderBy(Node node, List<SortItem> sortItems,
        Scope orderByScope) {
      ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();

      for (SortItem item : sortItems) {
        Expression expression = item.getSortKey();

        if (expression instanceof LongLiteral) {
          // this is an ordinal in the output tuple

          long ordinal = ((LongLiteral) expression).getValue();
          if (ordinal < 1 || ordinal > orderByScope.getRelationType().getVisibleFieldCount()) {
            throw new SemanticException(INVALID_ORDINAL, expression,
                "ORDER BY position %s is not in select list", ordinal);
          }

          expression = new FieldReference(toIntExact(ordinal - 1));
        }

        ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, orderByScope);
        analysis.recordSubqueries(node, expressionAnalysis);

        Type type = analysis.getType(expression);
        if (!type.isOrderable()) {
          throw new SemanticException(TYPE_MISMATCH, node,
              "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s", type,
              expression);
        }

        orderByFieldsBuilder.add(expression);
      }

      List<Expression> orderByFields = orderByFieldsBuilder.build();
      return orderByFields;
    }

    private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Field... fields) {
      return createAndAssignScope(node, parentScope, new RelationType(fields));
    }

    private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, List<Field> fields) {
      return createAndAssignScope(node, parentScope, new RelationType(fields));
    }

    private Scope createAndAssignScope(Node node, Optional<Scope> parentScope,
        RelationType relationType) {
      Scope scope = scopeBuilder(parentScope)
          .withRelationType(RelationId.of(node), relationType)
          .build();

      analysis.setScope(node, scope);
      return scope;
    }

    private Scope createScope(Optional<Scope> parentScope) {
      return scopeBuilder(parentScope).build();
    }

    private Scope.Builder scopeBuilder(Optional<Scope> parentScope) {
      Scope.Builder scopeBuilder = Scope.builder();

      if (parentScope.isPresent()) {
        // parent scope represents local query scope hierarchy. Local query scope
        // hierarchy should have outer query scope as ancestor already.
        scopeBuilder.withParent(parentScope.get());
      } else if (outerQueryScope.isPresent()) {
        scopeBuilder.withOuterQueryParent(outerQueryScope.get());
      }

      return scopeBuilder;
    }

    private class OrderByExpressionRewriter
        extends ExpressionRewriter<Void> {

      private final Multimap<QualifiedName, Expression> assignments;

      public OrderByExpressionRewriter(Multimap<QualifiedName, Expression> assignments) {
        this.assignments = assignments;
      }

      @Override
      public Expression rewriteIdentifier(Identifier reference, Void context,
          ExpressionTreeRewriter<Void> treeRewriter) {
        // if this is a simple name reference, try to resolve against output columns
        QualifiedName name = QualifiedName.of(reference.getValue());
        Set<Expression> expressions = assignments.get(name)
            .stream()
            .collect(Collectors.toSet());

        if (expressions.size() > 1) {
          throw new SemanticException(AMBIGUOUS_ATTRIBUTE, reference,
              "'%s' in ORDER BY is ambiguous", name);
        }

        if (expressions.size() == 1) {
          return Iterables.getOnlyElement(expressions);
        }

        // otherwise, couldn't resolve name against output aliases, so fall through...
        return reference;
      }
    }
  }

  protected static void verifyNoAggregateWindowOrGroupingFunctions(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, FunctionAndTypeManager functionAndTypeManager, Expression predicate, String clause)
  {
    List<FunctionCall> aggregates = extractAggregateFunctions(functionHandles, ImmutableList.of(predicate), functionAndTypeManager);

    List<GroupingOperation> groupingOperations = extractExpressions(ImmutableList.of(predicate), GroupingOperation.class);

    List<Expression> found = ImmutableList.copyOf(Iterables.concat(
        aggregates,
        groupingOperations));

    //todo now() is trigger this
    return;
//    if (!found.isEmpty()) {
//      throw new SemanticException(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, predicate, "%s cannot contain aggregations, window functions or grouping operations: %s", clause, found);
//    }
  }
}
