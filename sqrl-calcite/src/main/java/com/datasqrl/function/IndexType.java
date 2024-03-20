package com.datasqrl.function;

public enum IndexType {
  HASH, BTREE, TEXT, VEC_COSINE, VEC_EUCLID /*, VEC_PRODUCT */;

  public boolean hasStrictColumnOrder() {
    return this == HASH || this == BTREE;
  }

  public boolean requiresAllColumns() {
    return this == HASH;
  }

  /**
   * A general index covers comparison operators and can cover multiple columns
   * @return
   */
  public boolean isGeneralIndex() {
    return this == HASH || this==BTREE;
  }



}
