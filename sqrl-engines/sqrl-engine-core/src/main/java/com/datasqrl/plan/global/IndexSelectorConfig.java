/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.function.IndexType;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

public interface IndexSelectorConfig {

  double getCostImprovementThreshold();

  int maxIndexColumnSets();

  EnumSet<IndexType> supportedIndexTypes();

  int maxIndexColumns(IndexType indexType);

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
