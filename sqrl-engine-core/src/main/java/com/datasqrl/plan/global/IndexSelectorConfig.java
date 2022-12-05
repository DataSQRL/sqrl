/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import java.util.EnumSet;

public interface IndexSelectorConfig {

  double getCostImprovementThreshold();

  int maxIndexColumnSets();

  EnumSet<IndexDefinition.Type> supportedIndexTypes();

  int maxIndexColumns(IndexDefinition.Type indexType);

  double relativeIndexCost(IndexDefinition index);

  public static final IndexDefinition.Type[] PREFERRED_GENERIC_INDEX = {IndexDefinition.Type.BTREE, IndexDefinition.Type.HASH};

  default IndexDefinition.Type getPreferredGenericIndexType() {
    for (IndexDefinition.Type type : PREFERRED_GENERIC_INDEX) {
      if (supportedIndexTypes().contains(type)) return type;
    }
    throw new IllegalStateException("Does not support any preferred generic indexes");
  }

}
