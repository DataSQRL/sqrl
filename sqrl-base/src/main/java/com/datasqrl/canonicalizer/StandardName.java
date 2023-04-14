/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.canonicalizer;

import lombok.NonNull;

public class StandardName extends AbstractName {

  private String canonicalName;
  private String displayName;

  public StandardName() {
  } //For Kryo

  StandardName(@NonNull String canonicalName, @NonNull String displayName) {
    this.canonicalName = validateName(canonicalName);
    this.displayName = validateName(displayName);
  }

  @Override
  public String getCanonical() {
    return canonicalName;
  }

  @Override
  public String getDisplay() {
    return displayName;
  }


}
