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
package com.datasqrl.engine.database.relational;

import static com.datasqrl.plan.index.IndexType.BTREE;
import static com.datasqrl.plan.index.IndexType.HASH;
import static com.datasqrl.plan.index.IndexType.PBTREE;
import static com.datasqrl.plan.index.IndexType.TEXT;
import static com.datasqrl.plan.index.IndexType.VECTOR_COSINE;
import static com.datasqrl.plan.index.IndexType.VECTOR_EUCLID;

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson.IndexSelectionConfig;
import com.datasqrl.plan.index.IndexDefinition;
import com.datasqrl.plan.index.IndexSelectorConfig;
import com.datasqrl.plan.index.IndexType;
import com.google.common.base.Preconditions;
import java.util.EnumSet;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

/**
 * The index selection limits of a dialect. The tunable ones come from the {@code index-selection}
 * settings of the engine configuration.
 */
@AllArgsConstructor
@Value
@SuppressWarnings("ClassCanBeRecord")
public class IndexSelectorConfigByDialect implements IndexSelectorConfig {

  @NonNull JdbcDialect dialect;
  @NonNull IndexSelectionConfig indexSelection;

  @Override
  public double getCostImprovementThreshold() {
    return 1 - indexSelection.getMinCostReduction();
  }

  @Override
  public int getSmallTableRowLimit() {
    return indexSelection.getSmallTableRowLimit();
  }

  @Override
  public int getMinDistinctCount() {
    return indexSelection.getMinDistinctCount();
  }

  @Override
  public boolean hasPrimaryKeyIndex() {
    return switch (dialect) {
      case Postgres, MySQL, Oracle, H2, SQLite -> true;
      case Iceberg -> false;
      default -> throw new IllegalStateException("Dialect not supported:" + dialect.name());
    };
  }

  @Override
  public int maxIndexes() {
    return switch (dialect) {
      case Iceberg -> 1;
      default -> indexSelection.getMaxIndexes();
    };
  }

  @Override
  public int maxIndexColumnSets() {
    return switch (dialect) {
      case Iceberg -> Integer.MAX_VALUE;
      default -> indexSelection.getMaxFilterPatterns();
    };
  }

  @Override
  public EnumSet<IndexType> supportedIndexTypes() {
    return switch (dialect) {
      case Postgres, MySQL, Oracle -> EnumSet.of(HASH, BTREE, TEXT, VECTOR_COSINE, VECTOR_EUCLID);
      case H2, SQLite -> EnumSet.of(HASH, BTREE);
      case Iceberg -> EnumSet.of(PBTREE);
      default -> throw new IllegalStateException("Dialect not supported:" + dialect.name());
    };
  }

  @Override
  public int maxIndexColumns(IndexType indexType) {
    return switch (dialect) {
      case Postgres ->
          switch (indexType) {
            case HASH -> 1;
            default -> indexSelection.getMaxIndexColumns();
          };
      case MySQL, H2, SQLite, Iceberg -> indexSelection.getMaxIndexColumns();
      default -> throw new IllegalStateException("Dialect not supported: " + dialect.name());
    };
  }

  @Override
  public double relativeIndexCost(IndexDefinition index) {
    return switch (index.getType()) {
      case HASH -> 1;
      case BTREE -> 1.5 + 0.1 * index.getColumns().size();
      case PBTREE -> {
        Preconditions.checkArgument(index.getPartitionOffset() >= 0);
        yield 1 + 1.0 / (index.getPartitionOffset() + 1) + 0.1 * index.getColumns().size();
      }
      default -> 1;
    };
  }
}
