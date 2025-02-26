/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.type;

import java.io.Serializable;

public interface Type extends Serializable {

  default <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitType(this, context);
  }
}
