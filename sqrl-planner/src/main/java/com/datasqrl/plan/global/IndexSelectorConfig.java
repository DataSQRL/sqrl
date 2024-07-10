/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.function.IndexType;
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
   * The threshold in cost improvement. Once we cannot find an index that improves
   * cost by this threshold amount, we stop searching.
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
   * The maximum number of distinct indexing patterns to consider before creating
   * an individual index for each of the columns in those indexing patterns.
   *
   * Creating too many indexes is very expensive and the database will likely perform
   * better by combining multiple column indexes.
   *
   * @return
   */
  int maxIndexColumnSets();

  /**
   * A set of all supported indexes for this database.
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
      if (supportedIndexTypes().contains(type)) return type;
    }
    throw new IllegalStateException("Does not support any preferred generic indexes");
  }

  default Optional<IndexType> getPreferredSpecialIndexType(Set<IndexType> options) {
    return options.stream().filter(supportedIndexTypes()::contains).findFirst();
  }

}
