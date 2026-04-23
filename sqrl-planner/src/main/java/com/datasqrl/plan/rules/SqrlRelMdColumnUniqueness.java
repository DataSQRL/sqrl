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
package com.datasqrl.plan.rules;

import com.datasqrl.planner.TableAnalysisLookup;
import java.util.HashSet;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Column uniqueness handler that uses {@link TableAnalysisLookup} to detect primary keys. This
 * enables proper join cardinality estimation when joining on primary key columns.
 */
public class SqrlRelMdColumnUniqueness implements BuiltInMetadata.ColumnUniqueness.Handler {

  @Nullable private final TableAnalysisLookup tableLookup;

  public SqrlRelMdColumnUniqueness() {
    this(null);
  }

  public SqrlRelMdColumnUniqueness(@Nullable TableAnalysisLookup tableLookup) {
    this.tableLookup = tableLookup;
  }

  /** Generic dispatcher for RelNode. */
  @Override
  public Boolean areColumnsUnique(
      RelNode rel, RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {

    if (rel instanceof TableScan scan) {
      return areColumnsUnique(scan, mq, columns, ignoreNulls);
    }

    if (rel instanceof Filter filter) {
      return areColumnsUnique(filter, mq, columns, ignoreNulls);
    }

    if (rel instanceof Sort sort) {
      return areColumnsUnique(sort, mq, columns, ignoreNulls);
    }

    if (rel instanceof Exchange exchange) {
      return areColumnsUnique(exchange, mq, columns, ignoreNulls);
    }

    if (rel instanceof Project project) {
      return areColumnsUnique(project, mq, columns, ignoreNulls);
    }

    if (rel instanceof Aggregate aggregate) {
      return areColumnsUnique(aggregate, mq, columns, ignoreNulls);
    }

    // For unknown operators, return null (unknown)
    return null;
  }

  /**
   * Checks if columns are unique for a TableScan by looking up the primary key from TableAnalysis.
   */
  public Boolean areColumnsUnique(
      TableScan scan, RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {
    if (tableLookup == null) {
      return null; // Unknown
    }

    var tableAnalysis = tableLookup.lookupViewFromScan(scan);
    if (tableAnalysis != null) {
      var primaryKey = tableAnalysis.getPrimaryKey();
      if (primaryKey.isDefined()) {
        // Check if the provided columns cover the primary key
        var columnSet = new HashSet<>(columns.toList());
        if (primaryKey.coveredBy(columnSet)) {
          return true;
        }
      }
    }

    return null; // Unknown
  }

  /** Filter preserves uniqueness from input. */
  Boolean areColumnsUnique(
      Filter rel, RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {

    return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls);
  }

  /** Sort preserves uniqueness from input. */
  Boolean areColumnsUnique(
      Sort rel, RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {

    return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls);
  }

  /** Exchange preserves uniqueness from input. */
  Boolean areColumnsUnique(
      Exchange rel, RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {

    return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls);
  }

  /** Project: map columns back to input and check uniqueness. */
  Boolean areColumnsUnique(
      Project rel, RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {
    // Map output columns back to input columns
    var inputColumnsBuilder = ImmutableBitSet.builder();
    for (var col : columns) {
      if (col < rel.getProjects().size()) {
        var expr = rel.getProjects().get(col);
        if (expr instanceof RexInputRef inputRef) {
          inputColumnsBuilder.set(inputRef.getIndex());
        } else {
          // Non-simple projection, can't determine uniqueness
          return null;
        }
      }
    }

    var inputColumns = inputColumnsBuilder.build();
    if (inputColumns.isEmpty()) {
      return null;
    }

    return mq.areColumnsUnique(rel.getInput(), inputColumns, ignoreNulls);
  }

  /** Aggregate: grouping columns are unique if they cover the entire group set. */
  Boolean areColumnsUnique(
      Aggregate rel, RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {
    // The grouping columns form a unique key for the aggregate output
    var groupSet = rel.getGroupSet();
    if (columns.contains(groupSet)) {
      return true;
    }
    // If columns don't fully cover the group set, check if they include
    // columns that were unique in the input
    return null;
  }
}
