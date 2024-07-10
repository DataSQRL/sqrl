package com.datasqrl.function;

public enum IndexType {
  HASH, BTREE, PBTREE, TEXT, VEC_COSINE, VEC_EUCLID /*, VEC_PRODUCT */;

  public boolean requiresAllColumns() {
    return this == HASH;
  }

  /**
   * A general index covers comparison operators and can cover multiple columns.
   * If it is not a general index, it is a function index that has a specific indexing method.
   * @return
   */
  public boolean isGeneralIndex() {
    return this == HASH || this==BTREE || this == PBTREE;
  }

  public boolean isPartitioned() {
    return this == PBTREE;
  }



}
