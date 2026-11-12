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

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.function.IndexableFunction;
import com.datasqrl.plan.index.IndexSelector.NamedTable;
import com.datasqrl.plan.index.QueryIndexSummary.IndexableFunctionCall;
import com.datasqrl.plan.index.UnindexableFilter.Reason;
import com.datasqrl.util.FunctionUtil;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * Determines which conjuncts of a filter a database index can serve.
 *
 * <p>An index on a plain column serves a comparison only when the column stands alone on one side
 * and the other side does not depend on the row, like a parameter. A conjunct that would qualify if
 * its column stood alone becomes an {@link UnindexableFilter}. An index on the arguments of an
 * {@link IndexableFunction}, like a text search, serves a predicate on its call.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class FilterAnalyzer {

  /** The comparisons that an index on a plain column can serve. */
  private static final Set<SqlKind> INDEXABLE_COMPARISONS =
      EnumSet.of(
          SqlKind.EQUALS,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL);

  /** The arithmetic that can be moved to the other side of a comparison. */
  private static final Set<SqlKind> MOVABLE_ARITHMETIC =
      EnumSet.of(
          SqlKind.PLUS,
          SqlKind.MINUS,
          SqlKind.TIMES,
          SqlKind.DIVIDE,
          SqlKind.PLUS_PREFIX,
          SqlKind.MINUS_PREFIX);

  /**
   * Summarizes each disjunct of the disjunctive normal form of a filter.
   *
   * <p>A disjunct that compares columns with optional lookup parameters also gets a summary without
   * these columns, for the access when the database reads this table first (see {@link
   * LookupParameters}).
   */
  static IndexAnalysis analyze(NamedTable table, RexNode filter, SqrlRexUtil rexUtil) {
    List<QueryIndexSummary> summaries = new ArrayList<>();
    List<UnindexableFilter> unindexableFilters = new ArrayList<>();
    for (RexNode disjunct : rexUtil.getDisjunctions(rexUtil.toDnf(filter))) {
      List<Constraint> constraints = new ArrayList<>();
      for (RexNode conjunct : rexUtil.getConjunctions(disjunct)) {
        constraintOf(conjunct)
            .ifPresentOrElse(
                constraints::add,
                () -> unindexableFilterOf(table, conjunct).ifPresent(unindexableFilters::add));
      }
      summarize(table, constraints).ifPresent(summaries::add);
      if (constraints.stream().anyMatch(Constraint::optional)) {
        summarize(table, constraints.stream().filter(c -> !c.optional()).toList())
            .ifPresent(summaries::add);
      }
    }
    return new IndexAnalysis(summaries, unindexableFilters);
  }

  /** The indexable function call that an expression, like a sort key, consists of. */
  static Optional<IndexableFunctionCall> indexableFunctionCall(RexNode expression) {
    return findIndexableFunction(expression).flatMap(FilterAnalyzer::resolve);
  }

  /** A conjunct that an index can serve. */
  private sealed interface Constraint permits ColumnConstraint, FunctionConstraint {

    /** Whether the conjunct compares with an optional lookup parameter. */
    boolean optional();
  }

  private record ColumnConstraint(int column, boolean isEquality, boolean optional)
      implements Constraint {}

  private record FunctionConstraint(IndexableFunctionCall call, boolean optional)
      implements Constraint {}

  /** An operand of a comparison, and the value that the comparison compares it with. */
  private record Comparison(RexNode operand, RexNode value) {}

  private static Optional<Constraint> constraintOf(RexNode conjunct) {
    var functionCall = findIndexableFunction(conjunct);
    if (functionCall.isPresent()) {
      return functionCall
          .flatMap(FilterAnalyzer::resolve)
          .map(call -> new FunctionConstraint(call, containsOptionalLookup(conjunct)));
    }

    for (var comparison : comparisonsWithValue(conjunct)) {
      if (stripTransparentCasts(comparison.operand()) instanceof RexInputRef column) {
        return Optional.of(
            new ColumnConstraint(
                column.getIndex(),
                conjunct.isA(SqlKind.EQUALS),
                containsOptionalLookup(comparison.value())));
      }
    }

    return Optional.empty();
  }

  private static Optional<UnindexableFilter> unindexableFilterOf(
      NamedTable table, RexNode conjunct) {

    var functionCall = findIndexableFunction(conjunct);
    if (functionCall.isPresent()) {
      return functionCall
          .flatMap(FilterAnalyzer::expressionArgument)
          .map(argument -> new UnindexableFilter(table, Reason.EXPRESSION, argument, conjunct));
    }

    return comparisonsWithValue(conjunct).stream()
        .map(Comparison::operand)
        .filter(operand -> referencesColumn(operand) && !containsIndexableFunction(operand))
        .findFirst()
        .map(operand -> new UnindexableFilter(table, reasonFor(operand), operand, conjunct));
  }

  /**
   * The operands of an indexable comparison that it compares with a value that does not depend on
   * the row, like a parameter, a literal or an uncorrelated subquery.
   */
  private static List<Comparison> comparisonsWithValue(RexNode conjunct) {
    if (!(conjunct instanceof RexCall call) || !INDEXABLE_COMPARISONS.contains(call.getKind())) {
      return List.of();
    }

    var left = call.getOperands().get(0);
    var right = call.getOperands().get(1);
    return Stream.of(new Comparison(left, right), new Comparison(right, left))
        .filter(comparison -> !referencesRow(comparison.value()))
        .toList();
  }

  private static Optional<QueryIndexSummary> summarize(
      NamedTable table, List<Constraint> constraints) {

    if (constraints.isEmpty()) {
      return Optional.empty();
    }

    // IndexSelector breaks cost ties by the order of the index candidates, which follows the order
    // of these sets, so the order must be the same in every run: no Set.copyOf (which randomizes
    // the
    // order per JVM) and no hashing by object identity (like IndexableFunction)
    Set<Integer> equalityColumns = new HashSet<>();
    Set<Integer> inequalityColumns = new HashSet<>();
    Set<IndexableFunctionCall> functionCalls = new LinkedHashSet<>();
    for (var constraint : constraints) {
      if (constraint instanceof ColumnConstraint column) {
        (column.isEquality() ? equalityColumns : inequalityColumns).add(column.column());
      } else if (constraint instanceof FunctionConstraint function) {
        functionCalls.add(function.call());
      }
    }

    inequalityColumns.removeAll(equalityColumns); // only keep distinct inequalities

    return Optional.of(
        new QueryIndexSummary(
            table,
            ImmutableSet.copyOf(equalityColumns),
            ImmutableSet.copyOf(inequalityColumns),
            ImmutableSet.copyOf(functionCalls),
            1.0));
  }

  /**
   * The only {@link IndexableFunction} call of a predicate, either the predicate itself or within
   * comparisons and arithmetic, like {@code cosine_similarity(embedding, :vector) > 0.8}, as long
   * as the predicate references no column outside of the call.
   */
  private static Optional<RexCall> findIndexableFunction(RexNode predicate) {
    if (!(predicate instanceof RexCall call)) {
      return Optional.empty();
    }

    if (indexableFunction(call).isPresent()) {
      return Optional.of(call);
    }

    if (!call.isA(SqlKind.BINARY_ARITHMETIC) && !call.isA(SqlKind.BINARY_COMPARISON)) {
      return Optional.empty();
    }

    RexCall functionCall = null;
    for (RexNode operand : call.getOperands()) {
      var operandCall = findIndexableFunction(operand);
      if (operandCall.isPresent() && functionCall == null) {
        functionCall = operandCall.get();
      } else if (operandCall.isPresent() || referencesColumn(operand)) {
        return Optional.empty();
      }
    }

    return Optional.ofNullable(functionCall);
  }

  /** The call of an indexable function whose index arguments are all plain columns. */
  private static Optional<IndexableFunctionCall> resolve(RexCall call) {
    var function = indexableFunction(call).orElseThrow();
    var operandSelector = function.getOperandSelector();

    List<Integer> columns = new ArrayList<>();
    for (var i = 0; i < call.getOperands().size(); i++) {
      var operand = call.getOperands().get(i);
      if (operandSelector.isSelectableColumn(i) && operand instanceof RexInputRef column) {
        columns.add(column.getIndex());
      } else if (referencesColumn(operand)) {
        return Optional.empty();
      }
    }

    if (columns.isEmpty() || columns.size() > operandSelector.maxNumberOfColumns()) {
      return Optional.empty();
    }

    return Optional.of(new IndexableFunctionCall(columns, function));
  }

  /**
   * The first index argument of an indexable function call that wraps columns in an expression, as
   * long as no other argument references a column.
   */
  private static Optional<RexNode> expressionArgument(RexCall call) {
    var operandSelector = indexableFunction(call).orElseThrow().getOperandSelector();
    var operands = call.getOperands();
    var columnArguments =
        IntStream.range(0, operands.size())
            .filter(i -> referencesColumn(operands.get(i)))
            .boxed()
            .toList();

    if (!columnArguments.stream().allMatch(operandSelector::isSelectableColumn)) {
      return Optional.empty();
    }

    return columnArguments.stream()
        .map(operands::get)
        .filter(argument -> !(argument instanceof RexInputRef))
        .findFirst();
  }

  private static Reason reasonFor(RexNode wrappedColumn) {
    var expression = stripTransparentCasts(wrappedColumn);
    if (expression.isA(SqlKind.CAST)
        && stripTransparentCasts(((RexCall) expression).getOperands().get(0))
            instanceof RexInputRef) {
      return Reason.CAST;
    }

    if (SqrlRexUtil.findAllInputRefs(List.of(expression)).size() == 1
        && isMovableArithmetic(expression)) {
      return Reason.ARITHMETIC;
    }

    return Reason.EXPRESSION;
  }

  private static boolean isMovableArithmetic(RexNode node) {
    if (!(node instanceof RexCall call)) {

      return true;
    }
    return (MOVABLE_ARITHMETIC.contains(call.getKind()) || isTransparentCast(call))
        && call.getOperands().stream().allMatch(FilterAnalyzer::isMovableArithmetic);
  }

  /**
   * Removes the casts that Postgres reduces to a relabeling of the column, so that an index on the
   * column still applies: casts between character types, and casts that only change the precision
   * or nullability of a type.
   */
  private static RexNode stripTransparentCasts(RexNode node) {
    while (node instanceof RexCall call && isTransparentCast(call)) {
      node = call.getOperands().get(0);
    }
    return node;
  }

  private static boolean isTransparentCast(RexCall call) {
    if (!call.isA(SqlKind.CAST)) {
      return false;
    }

    var from = call.getOperands().get(0).getType();
    var to = call.getType();

    return from.getSqlTypeName() == to.getSqlTypeName()
        || (SqlTypeUtil.inCharFamily(from) && SqlTypeUtil.inCharFamily(to));
  }

  private static Optional<IndexableFunction> indexableFunction(RexCall call) {
    return FunctionUtil.getBridgedFunction(call.getOperator())
        .flatMap(function -> FunctionUtil.getFunctionMetaData(function, IndexableFunction.class));
  }

  private static boolean referencesColumn(RexNode node) {
    return anyMatch(node, RexInputRef.class::isInstance);
  }

  /**
   * Whether the value depends on the row: it references a column, or contains a subquery that is
   * correlated with the row.
   */
  private static boolean referencesRow(RexNode node) {
    return anyMatch(
        node,
        part ->
            part instanceof RexInputRef
                || (part instanceof RexSubQuery subQuery
                    && !RelOptUtil.getVariablesUsed(subQuery.rel).isEmpty()));
  }

  private static boolean containsOptionalLookup(RexNode node) {
    return anyMatch(node, LookupParameters::isOptionalLookup);
  }

  private static boolean containsIndexableFunction(RexNode node) {
    return anyMatch(
        node, part -> part instanceof RexCall call && indexableFunction(call).isPresent());
  }

  private static boolean anyMatch(RexNode node, Predicate<RexNode> predicate) {
    if (predicate.test(node)) {
      return true;
    }

    if (node instanceof RexFieldAccess access) {
      return anyMatch(access.getReferenceExpr(), predicate);
    }

    return node instanceof RexCall call
        && call.getOperands().stream().anyMatch(operand -> anyMatch(operand, predicate));
  }
}
