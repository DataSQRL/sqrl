/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.input;

import com.datasqrl.io.schema.flexible.type.Type;
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
