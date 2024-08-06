/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.canonicalizer;

public class SimpleName extends AbstractName {

  private final String name;

  SimpleName(String name) {
    this.name = validateName(name);
  }

  @Override
  public String getCanonical() {
    return name;
  }

  @Override
  public String getDisplay() {
    return name;
  }
}
