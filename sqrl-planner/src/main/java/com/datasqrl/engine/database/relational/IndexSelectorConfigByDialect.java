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

import static com.datasqrl.plan.global.IndexType.BTREE;
import static com.datasqrl.plan.global.IndexType.HASH;
import static com.datasqrl.plan.global.IndexType.PBTREE;
import static com.datasqrl.plan.global.IndexType.TEXT;
import static com.datasqrl.plan.global.IndexType.VECTOR_COSINE;
import static com.datasqrl.plan.global.IndexType.VECTOR_EUCLID;

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.IndexType;
import com.google.common.base.Preconditions;
import java.util.EnumSet;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class IndexSelectorConfigByDialect implements IndexSelectorConfig {

  public static final double DEFAULT_COST_THRESHOLD = 0.95;
  public static final int MAX_INDEX_COLUMNS = 3;

  @Getter @Builder.Default double costImprovementThreshold = DEFAULT_COST_THRESHOLD;
  @NonNull JdbcDialect dialect;
  @Builder.Default int maxIndexColumns = MAX_INDEX_COLUMNS;

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
      default -> 8;
    };
  }

  @Override
  public int maxIndexColumnSets() {
    return switch (dialect) {
      case Iceberg -> Integer.MAX_VALUE;
      default -> 50;
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
    switch (dialect) {
      case Postgres:
        return switch (indexType) {
          case HASH -> 1;
          default -> maxIndexColumns;
        };
      case MySQL:
      case H2:
      case SQLite:
      case Iceberg:
        return maxIndexColumns;
      default:
        throw new IllegalStateException("Dialect not supported: " + dialect.name());
    }
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

  public static IndexSelectorConfigByDialect of(JdbcDialect dialect) {
    return IndexSelectorConfigByDialect.builder().dialect(dialect).build();
  }
}
