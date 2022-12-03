package com.datasqrl.schema.type.basic;

import com.datasqrl.schema.type.SqrlTypeVisitor;

public abstract class AbstractBasicType<J> implements BasicType<J> {

  AbstractBasicType() {
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
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

  public int compareTo(BasicType o) {
    return getName().compareTo(o.getName());
  }

  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitBasicType(this, context);
  }
}
