/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.canonicalizer;

//import com.google.common.base.Preconditions;
//import com.google.common.base.Strings;

public abstract class AbstractName implements Name {

  public static String validateName(String name) {
//    Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Not a valid name: %s", name);
    return name;
  }

  @Override
  public String toString() {
    return getDisplay();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    } else if (this == other) {
      return true;
    } else if (!(other instanceof Name)) {
      return false;
    }
    var o = (Name) other;
    return getCanonical().equals(o.getCanonical());
  }

  @Override
  public int hashCode() {
    return getCanonical().hashCode();
  }

  @Override
  public int compareTo(Name o) {
    return getCanonical().compareTo(o.getCanonical());
  }

  @Override
  public boolean hasPrefix(Name variablePrefix) {
    return this.getCanonical().startsWith(variablePrefix.getCanonical());
  }
}
