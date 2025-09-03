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
package com.datasqrl.plan.global;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.function.IndexableFunction;
import com.datasqrl.plan.global.IndexSelector.NamedTable;
import com.datasqrl.plan.rules.SqrlRelMdRowCount;
import com.datasqrl.util.FunctionUtil;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.table.functions.FunctionDefinition;

/**
 * This class represents the potentially indexable filters and sorts of a query. Those include
 * equality and inequality constraints on a single column and {@link IndexableFunction} calls. <br>
 * This class provides the methods to create a {@link QueryIndexSummary} from a WHERE clause (i.e.
 * {@link org.apache.calcite.rel.logical.LogicalFilter}) and estimating the cost of a {@link
 * QueryIndexSummary} against an {@link IndexDefinition}.
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class QueryIndexSummary {

  private static final QueryIndexSummary EMPTY =
      new QueryIndexSummary(null, Set.of(), Set.of(), Set.of(), 1.0);

  public static final String INDEX_NAME = "_index_";

  @Include NamedTable table;
  @Include Set<Integer> equalityColumns;
  @Include Set<Integer> inequalityColumns;
  @Include Set<IndexableFunctionCall> functionCalls;

  // TODO: add support for sort orders
  // List<IndexableSort> sorts;

  /** Keeps track of the relative frequency of query conjunctions as we reduce them */
  double count = 1.0;

  public static List<QueryIndexSummary> ofFilter(
      @NonNull NamedTable table, RexNode filter, SqrlRexUtil rexUtil) {
    List<QueryIndexSummary> indexSummaries = new ArrayList<>();
    var dnf = rexUtil.toDnf(filter);
    for (RexNode conjunction : rexUtil.getDisjunctions(dnf)) {
      var conjunctions = rexUtil.getConjunctions(conjunction);
      Set<Integer> equalityColumns = new HashSet<>();
      Set<Integer> inequalityColumns = new HashSet<>();
      Set<IndexableFunctionCall> functionCalls = new HashSet<>();
      for (RexNode conj : conjunctions) {
        if (conj instanceof RexCall call) {
          var idxFinder = new IndexableFinder();
          call.accept(idxFinder);
          if (idxFinder.isIndexable && (idxFinder.idxCall != null ^ idxFinder.columnRef != null)) {
            if (idxFinder.idxCall != null) {
              functionCalls.add(idxFinder.idxCall);
            } else {
              (call.isA(SqlKind.EQUALS) ? equalityColumns : inequalityColumns)
                  .add(idxFinder.columnRef);
            }
          }
        }
      }
      if (!equalityColumns.isEmpty() || !inequalityColumns.isEmpty() || !functionCalls.isEmpty()) {
        inequalityColumns.removeAll(equalityColumns); // only keep distinct inequalities
        indexSummaries.add(
            new QueryIndexSummary(
                table,
                ImmutableSet.copyOf(equalityColumns),
                ImmutableSet.copyOf(inequalityColumns),
                ImmutableSet.copyOf(functionCalls),
                1.0));
      }
    }
    return indexSummaries;
  }

  public static Optional<QueryIndexSummary> ofSort(@NonNull NamedTable table, RexNode node) {
    if (node instanceof RexCall call) {
      var idxFinder = new IndexableFinder();
      call.accept(idxFinder);
      if (idxFinder.isIndexable && idxFinder.idxCall != null) {
        return Optional.of(
            new QueryIndexSummary(
                table, Set.of(), Set.of(), ImmutableSet.of(idxFinder.idxCall), 1.0));
      }
    }
    return Optional.empty();
  }

  public static Optional<QueryIndexSummary> ofSort(@NonNull NamedTable table, int columnIndex) {
    return Optional.of(
        new QueryIndexSummary(table, Set.of(), ImmutableSet.of(columnIndex), Set.of(), 1.0));
  }

  public double getCost(@NonNull IndexDefinition indexDef) {
    var indexType = indexDef.getType();
    QueryIndexSummary coveredConjunction;
    if (indexType.isGeneralIndex()) {
      Set<Integer> equalityCols = new HashSet<>();
      Set<Integer> inequalityCols = new HashSet<>();

      var i = 0;
      for (; i < indexDef.getColumns().size(); i++) {
        int colIndex = indexDef.getColumns().get(i);
        if (this.equalityColumns.contains(colIndex)) {
          equalityCols.add(colIndex);
        } else {
          if (this.inequalityColumns.contains(colIndex)) {
            inequalityCols.add(colIndex);
          }
          break; // we have broken the equality chain of this index
        }
      }
      if (i < indexDef.numEqualityColumnsRequired()) {
        // This index requires a constraint on all columns to be invocable
        coveredConjunction = EMPTY;
      } else {
        coveredConjunction =
            new QueryIndexSummary(this.table, equalityCols, inequalityCols, Set.of(), this.count);
      }
    } else {
      // See which of the indexable function calls are covered
      List<IndexableFunctionCall> coveredCalls = new ArrayList<>();
      Set<Integer> indexCols = ImmutableSet.copyOf(indexDef.getColumns());
      for (IndexableFunctionCall fcall : this.functionCalls) {
        var function = fcall.function();
        if (function.getSupportedIndexes().contains(indexType)
            && indexCols.containsAll(fcall.columnIndexes())) {
          coveredCalls.add(fcall);
        }
      }
      if (coveredCalls.isEmpty()) {
        coveredConjunction = EMPTY;
      } else {
        coveredConjunction =
            new QueryIndexSummary(
                this.table, Set.of(), Set.of(), ImmutableSet.copyOf(coveredCalls), this.count);
      }
    }
    return SqrlRelMdRowCount.getRowCount(table.getAnalysis(), coveredConjunction);
  }

  public double getBaseCost() {
    return SqrlRelMdRowCount.getRowCount(table.getAnalysis(), EMPTY);
  }

  @Override
  public String toString() {
    return table.getTableId()
        + "eq"
        + equalityColumns.toString()
        + "iq"
        + inequalityColumns.toString()
        + functionCalls.toString();
  }

  public record IndexableFunctionCall(List<Integer> columnIndexes, IndexableFunction function) {}

  /**
   * Visits a RexCall to determine whether this predicate is indexable. This visitor will look for a
   * single RexInputRef within an arithmetic expression or a single IndexableFunction call within an
   * arithmetic expression.
   *
   * <p>If it finds multiple such occurrences or occurrences outside arithmetic expressions it will
   * consider the RexCall to not be indexable.
   */
  private static class IndexableFinder extends RexShuttle {

    private Integer columnRef = null;
    private IndexableFunctionCall idxCall = null;

    private boolean parentIsArithmetic = true;
    private boolean isIndexable = true;

    @Override
    public RexNode visitInputRef(RexInputRef input) {
      if (!parentIsArithmetic || columnRef != null || idxCall != null) {
        isIndexable = false;
      } else {
        columnRef = input.getIndex();
      }
      return input;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      var prior = parentIsArithmetic;
      Optional<IndexableFunction> sqrlFunction =
          FunctionUtil.getBridgedFunction(call.getOperator())
              .flatMap(QueryIndexSummary.IndexableFinder::getIndexableFunction);
      if (sqrlFunction.isPresent() && parentIsArithmetic) {
        // This is either a top level predicate or a distance function inside a comparison
        var idxFunction = sqrlFunction.get();
        var optCall = resolveIndexFunctionCall(call, idxFunction);
        if (optCall.isPresent()) {
          if (columnRef != null || idxCall != null) {
            isIndexable = false;
          } else {
            idxCall = optCall.get();
          }
          return call; // Don't need to go into the call
        }
      }
      if (!call.isA(SqlKind.BINARY_ARITHMETIC) && !call.isA(SqlKind.BINARY_COMPARISON)) {
        parentIsArithmetic = false;
      }
      var result = super.visitCall(call);
      parentIsArithmetic = prior;
      return result;
    }

    private static Optional<IndexableFunction> getIndexableFunction(
        FunctionDefinition functionDefinition) {
      return FunctionUtil.getFunctionMetaData(functionDefinition, IndexableFunction.class);
    }

    private static Optional<IndexableFunctionCall> resolveIndexFunctionCall(
        RexCall call, IndexableFunction idxFunction) {
      List<RexNode> remainingOperands = new ArrayList<>();
      List<Integer> columnIndexes = new ArrayList<>();
      var operands = call.getOperands();
      var operandSelector = idxFunction.getOperandSelector();
      for (var i = 0; i < operands.size(); i++) {
        var node = operands.get(i);
        if (operandSelector.isSelectableColumn(i) && (node instanceof RexInputRef ref)) {
          columnIndexes.add(ref.getIndex());
        } else {
          remainingOperands.add(node);
        }
      }
      if (columnIndexes.isEmpty()
          || columnIndexes.size() > operandSelector.maxNumberOfColumns()
          || !SqrlRexUtil.findAllInputRefs(remainingOperands).isEmpty()) {
        // If the remainingOperands contain RexInputRef this isn't an indexable call
        // TODO: issue warning since this is likely not desired
        return Optional.empty();
      }
      return Optional.of(new IndexableFunctionCall(columnIndexes, idxFunction));
    }
  }
}
