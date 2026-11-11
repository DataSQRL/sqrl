/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
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
package com.datasqrl.plan.index;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Splits a query into the queries that index selection analyzes: the query itself and the query of
 * each of its subqueries ({@link RexSubQuery}), including nested ones. In the query of a subquery,
 * the correlated columns of the outer query become lookup parameters, which the database binds when
 * it evaluates the subquery for a row of the outer query.
 *
 * <p>An {@code EXISTS} or {@code IN} subquery that is a conjunct of a filter becomes a semi join:
 * Postgres pulls it up into the join tree, the same way Flink rewrites it into a semi join. When
 * all conditions that join the subquery to the outer query are equalities, the database can read
 * either table first and look up the other one. So the filter replaces the subquery with
 * comparisons of the outer columns that the subquery correlates with to optional lookup parameters,
 * and the parameters of the subquery are optional, too. Any other join condition, like {@code o.ts
 * > c.ts}, makes the database read the outer table first, so the outer table gets no lookup.
 *
 * <p>The database evaluates any other subquery, like {@code NOT EXISTS}, {@code NOT IN}, a scalar
 * subquery or a subquery in a disjunction, once or for each row of the outer query, so it
 * constrains only the subquery's tables. Its parameters are optional only for an {@code EXISTS},
 * which Postgres can also evaluate as an anti join or a hashed subplan.
 *
 * <p>The subqueries are resolved before the filters are pushed down: FilterProjectTransposeRule
 * does not move a filter that references a correlation variable, and it moves a filter with a
 * correlated subquery without updating the field indexes of the variable.
 */
@RequiredArgsConstructor
final class SubQueryExpander {

  private static final Set<SqlKind> SEMI_JOINS = EnumSet.of(SqlKind.EXISTS, SqlKind.IN);

  private final RexBuilder rexBuilder;
  private final LookupParameters parameters;

  /** The query without its semi join subqueries, followed by the queries of all its subqueries. */
  List<RelNode> expand(RelNode query) {
    List<RelNode> queries = new ArrayList<>();
    var pending = new ArrayDeque<RelNode>(List.of(query));
    while (!pending.isEmpty()) {
      var extractor = new SubQueryExtractor();
      queries.add(pending.poll().accept(extractor));
      pending.addAll(extractor.subQueries);
    }
    return queries;
  }

  /** Removes the semi join subqueries of a query, and collects the queries of its subqueries. */
  private class SubQueryExtractor extends RelHomogeneousShuttle {

    private final List<RelNode> subQueries = new ArrayList<>();

    private final RexShuttle subQueryCollector =
        new RexShuttle() {
          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            subQueries.add(bindCorrelations(subQuery.rel, subQuery.isA(SqlKind.EXISTS)));
            return super.visitSubQuery(subQuery);
          }
        };

    @Override
    public RelNode visit(RelNode other) {
      var node = super.visit(other);
      if (node instanceof Filter filter) {
        return removeSemiJoins(filter);
      }
      node.accept(subQueryCollector);
      return node;
    }

    private RelNode removeSemiJoins(Filter filter) {
      List<RexNode> conjuncts = new ArrayList<>();
      for (RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
        if (conjunct instanceof RexSubQuery subQuery && subQuery.isA(SEMI_JOINS)) {
          outerColumns(filter, subQuery).stream()
              .map(SubQueryExpander.this::equalsOptionalLookup)
              .forEach(conjuncts::add);
          subQueries.add(semiJoinQuery(subQuery));
        } else {
          conjuncts.add(conjunct);
        }
      }

      var condition = RexUtil.composeConjunction(rexBuilder, conjuncts);
      condition.accept(subQueryCollector);

      return condition.isAlwaysTrue()
          ? filter.getInput()
          : filter.copy(filter.getTraitSet(), filter.getInput(), condition);
    }
  }

  /** The query of a semi join subquery, which the database looks up by the values of an IN. */
  private RelNode semiJoinQuery(RexSubQuery subQuery) {
    var query = bindCorrelations(subQuery.rel, true);
    if (!subQuery.isA(SqlKind.IN)) {
      return query;
    }

    var inValues =
        IntStream.range(0, subQuery.getOperands().size())
            .mapToObj(i -> equalsOptionalLookup(rexBuilder.makeInputRef(query, i)))
            .toList();

    return LogicalFilter.create(query, RexUtil.composeConjunction(rexBuilder, inValues));
  }

  /**
   * The columns of the filter input that a semi join subquery compares with its own columns: the
   * values of an {@code IN}, or the correlated columns of an {@code EXISTS}, like {@code
   * c.customer_id} in {@code EXISTS (SELECT 1 FROM Owners o WHERE o.customer_id = c.customer_id)}.
   */
  private List<RexNode> outerColumns(Filter filter, RexSubQuery subQuery) {
    if (subQuery.isA(SqlKind.IN)) {
      return subQuery.getOperands();
    }

    var correlations = filter.getVariablesSet();
    var joinConditions =
        topConjuncts(subQuery.rel)
            .filter(conjunct -> isJoinCondition(conjunct, correlations))
            .toList();
    // Postgres starts a semi join from the subquery side only when all its join conditions are
    // equalities, otherwise it scans the outer table, and an index on an outer column goes unused
    if (!joinConditions.stream().allMatch(condition -> condition.isA(SqlKind.EQUALS))) {
      return List.of();
    }

    return joinConditions.stream()
        .flatMap(condition -> correlatedField(condition, correlations).stream())
        .map(field -> (RexNode) rexBuilder.makeInputRef(filter.getInput(), field))
        .toList();
  }

  /** Whether a conjunct of the subquery compares its own columns with the outer row. */
  private static boolean isJoinCondition(RexNode conjunct, Set<CorrelationId> correlations) {

    var variables = new RelOptUtil.VariableUsedVisitor(null);
    conjunct.accept(variables);
    return RexUtil.containsInputRef(conjunct)
        && !Collections.disjoint(variables.variables, correlations);
  }

  /** The conjuncts of the filters at the top of a query, above its first join or aggregation. */
  private static Stream<RexNode> topConjuncts(RelNode query) {
    List<RexNode> conjuncts = new ArrayList<>();
    for (var node = query;
        node instanceof Project || node instanceof Filter;
        node = node.getInput(0)) {

      if (node instanceof Filter filter) {
        conjuncts.addAll(RelOptUtil.conjunctions(filter.getCondition()));
      }
    }

    return conjuncts.stream();
  }

  /** The field of {@code inner = $cor.field}, for a correlation variable of the outer filter. */
  private static Optional<Integer> correlatedField(
      RexNode conjunct, Set<CorrelationId> correlations) {

    if (!(conjunct instanceof RexCall equals) || !equals.isA(SqlKind.EQUALS)) {
      return Optional.empty();
    }

    var left = equals.getOperands().get(0);
    var right = equals.getOperands().get(1);

    return correlatedField(left, right, correlations)
        .or(() -> correlatedField(right, left, correlations));
  }

  private static Optional<Integer> correlatedField(
      RexNode outer, RexNode inner, Set<CorrelationId> correlations) {

    if (outer instanceof RexFieldAccess access
        && access.getReferenceExpr() instanceof RexCorrelVariable variable
        && correlations.contains(variable.id)
        && !RexUtil.containsCorrelation(inner)) {

      return Optional.of(access.getField().getIndex());
    }

    return Optional.empty();
  }

  private RexNode equalsOptionalLookup(RexNode column) {
    return rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS, column, parameters.optionalLookup(column.getType()));
  }

  /**
   * Replaces the correlation variables that the query references, but does not set itself, with
   * lookup parameters.
   *
   * @param optional whether the database can also evaluate the query without the correlation
   */
  private RelNode bindCorrelations(RelNode query, boolean optional) {
    return query.accept(new CorrelationBinder(RelOptUtil.getVariablesUsed(query), optional));
  }

  @RequiredArgsConstructor
  private class CorrelationBinder extends RelHomogeneousShuttle {

    private final Set<CorrelationId> correlations;
    private final boolean optional;

    private final RexShuttle rexShuttle =
        new RexShuttle() {
          @Override
          public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable variable
                && correlations.contains(variable.id)) {
              var type = fieldAccess.getType();
              return optional ? parameters.optionalLookup(type) : parameters.lookup(type);
            }

            return super.visitFieldAccess(fieldAccess);
          }

          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            var rel = subQuery.rel.accept(CorrelationBinder.this);

            return super.visitSubQuery(rel == subQuery.rel ? subQuery : subQuery.clone(rel));
          }
        };

    @Override
    public RelNode visit(RelNode other) {
      return super.visit(other).accept(rexShuttle);
    }
  }
}
