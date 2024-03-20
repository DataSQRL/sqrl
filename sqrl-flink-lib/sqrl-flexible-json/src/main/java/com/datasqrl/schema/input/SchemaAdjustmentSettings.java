/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;


import java.io.Serializable;

public interface SchemaAdjustmentSettings extends Serializable {

  SchemaAdjustmentSettings DEFAULT = new SchemaAdjustmentSettings() {
  };

  default boolean deepenArrays() {
    return true;
  }

  default boolean removeListNulls() {
    return true;
  }

  default boolean null2EmptyArray() {
    return true;
  }

  default boolean castDataType() {
    return true;
  }

  default int maxCastingTypeDistance() {
    if (!castDataType()) {
      return 0;
    }
    return 10;
  }

  default boolean forceCastDataType() {
    return castDataType();
  }

  default int maxForceCastingTypeDistance() {
    if (!forceCastDataType()) {
      return maxCastingTypeDistance();
    }
    return Integer.MAX_VALUE;
  }

  default boolean dropFields() {
    return true;
  }


}
