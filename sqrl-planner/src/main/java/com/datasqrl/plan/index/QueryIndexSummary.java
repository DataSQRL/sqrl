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

import com.datasqrl.function.IndexableFunction;
import com.datasqrl.plan.index.IndexSelector.NamedTable;
import com.datasqrl.plan.rules.SqrlRelMdRowCount;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

/**
 * This class represents the potentially indexable filters and sorts of a query. Those include
 * equality and inequality constraints on a single column and {@link IndexableFunction} calls. <br>
 * {@link FilterAnalyzer} creates them from a WHERE clause (i.e. {@link
 * org.apache.calcite.rel.logical.LogicalFilter}). This class provides the methods to create a
 * {@link QueryIndexSummary} from a sort and estimating the cost of a {@link QueryIndexSummary}
 * against an {@link IndexDefinition}.
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class QueryIndexSummary {

  private static final QueryIndexSummary EMPTY =
      new QueryIndexSummary(null, Set.of(), Set.of(), Set.of(), 1.0);

  @Include NamedTable table;
  @Include Set<Integer> equalityColumns;
  @Include Set<Integer> inequalityColumns;
  @Include Set<IndexableFunctionCall> functionCalls;

  /** Keeps track of the relative frequency of query conjunctions as we reduce them */
  double count;

  public static Optional<QueryIndexSummary> ofSort(@NonNull NamedTable table, RexNode node) {
    if (node instanceof RexInputRef inputRef) {
      return ofSort(table, inputRef.getIndex());
    }
    return FilterAnalyzer.indexableFunctionCall(node)
        .map(call -> new QueryIndexSummary(table, Set.of(), Set.of(), ImmutableSet.of(call), 1.0));
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
      for (IndexableFunctionCall fnCall : this.functionCalls) {
        var function = fnCall.function();
        if (function.getSupportedIndexes().contains(indexType)
            && indexCols.containsAll(fnCall.columnIndexes())) {
          coveredCalls.add(fnCall);
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

  public List<String> getFilterColumnNames() {
    var fieldNames = table.getAnalysis().getRowType().getFieldNames();
    return Stream.of(
            equalityColumns.stream(),
            inequalityColumns.stream(),
            functionCalls.stream().flatMap(fcall -> fcall.columnIndexes().stream()))
        .flatMap(Function.identity())
        .distinct()
        .sorted()
        .map(fieldNames::get)
        .collect(Collectors.toList());
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
}
