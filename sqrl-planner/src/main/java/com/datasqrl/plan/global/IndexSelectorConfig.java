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

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

public interface IndexSelectorConfig {

  /**
   * If the database has a primary key index by default.
   *
   * @return
   */
  boolean hasPrimaryKeyIndex();

  /**
   * The maximum fraction of their prior cost that the queries served by an index may still cost
   * once that index exists. An index candidate is only created if it improves the queries it serves
   * by at least this much - unrelated queries on the same table do not count towards it.
   *
   * @return
   */
  double getCostImprovementThreshold();

  /**
   * The maximum number of indexes to create per table
   *
   * @return
   */
  int maxIndexes();

  /**
   * The maximum number of distinct indexing patterns to consider before creating an individual
   * index for each of the columns in those indexing patterns.
   *
   * <p>Creating too many indexes is very expensive and the database will likely perform better by
   * combining multiple column indexes.
   *
   * @return
   */
  int maxIndexColumnSets();

  /**
   * A set of all supported indexes for this database.
   *
   * @return
   */
  EnumSet<IndexType> supportedIndexTypes();

  /**
   * The maximum number of columns that can be indexed by this type of index.
   *
   * @param indexType
   * @return
   */
  int maxIndexColumns(IndexType indexType);

  /**
   * The relative cost of creating this index against some fixed baseline.
   *
   * @param index
   * @return
   */
  double relativeIndexCost(IndexDefinition index);

  public static final IndexType[] PREFERRED_GENERIC_INDEX = {IndexType.BTREE, IndexType.HASH};

  default IndexType getPreferredGenericIndexType() {
    for (IndexType type : PREFERRED_GENERIC_INDEX) {
      if (supportedIndexTypes().contains(type)) {
        return type;
      }
    }
    throw new IllegalStateException("Does not support any preferred generic indexes");
  }

  default Optional<IndexType> getPreferredSpecialIndexType(Set<IndexType> options) {
    return options.stream().filter(supportedIndexTypes()::contains).findFirst();
  }
}
