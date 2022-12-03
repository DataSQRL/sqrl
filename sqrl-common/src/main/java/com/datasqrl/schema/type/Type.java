package com.datasqrl.schema.type;

import java.io.Serializable;

public interface Type extends Serializable {

  default <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitType(this, context);
  }

}
