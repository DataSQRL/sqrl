/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.ReservedName;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;

@Getter
//@EqualsAndHashCode do not use
public abstract class Field {

  @NonNull
  protected final Name name;
  protected final int version;

  protected Field(@NonNull Name name, int version) {
    Preconditions.checkArgument(version >= 0);
    this.name = name;
    this.version = version;
  }

  public Name getId() {
    return name.suffix(Integer.toString(version));
  }

  public boolean isVisible() {
    return true;
  }

  @Override
  public String toString() {
    return getId().toString();
  }

  public abstract <R, C> R accept(FieldVisitor<R, C> visitor, C context);
}