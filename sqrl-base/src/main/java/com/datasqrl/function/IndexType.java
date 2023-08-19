package com.datasqrl.function;

public enum IndexType {
  HASH, BTREE, TEXT;

  public boolean hasStrictColumnOrder() {
    return this == HASH || this == BTREE;
  }

  public boolean requiresAllColumns() {
    return this == HASH;
  }

  public boolean isSpecialIndex() {
    return this == TEXT;
  }

  public boolean isGeneralIndex() {
    return !isSpecialIndex();
  }



}
