/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.type.basic;

import com.datasqrl.io.schema.flexible.type.SqrlTypeVisitor;

public abstract class AbstractBasicType<J> implements BasicType<J> {

  AbstractBasicType() {
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public String getName() {
    return getNames().get(0);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractBasicType<?> that = (AbstractBasicType<?>) o;
    return getName().equals(that.getName());
  }

  @Override
  public String toString() {
    return getName();
  }

  public int compareTo(BasicType<?> o) {
    return getName().compareTo(o.getName());
  }

  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitBasicType(this, context);
  }
}
