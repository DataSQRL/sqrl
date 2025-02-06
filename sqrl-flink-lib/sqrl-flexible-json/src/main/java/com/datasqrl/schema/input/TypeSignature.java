/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import com.datasqrl.schema.type.Type;

import lombok.NonNull;
import lombok.Value;

public interface TypeSignature {

  Type getRaw();

  Type getDetected();

  int getArrayDepth();

  @Value
  class Simple implements TypeSignature {

    @NonNull
    private final Type raw;
    @NonNull
    private final Type detected;
    private final int arrayDepth;

  }
}
