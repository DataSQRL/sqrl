/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.canonicalizer;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class IdentityCanonicalizer implements NameCanonicalizer {

  @Override
  public String getCanonical(String name) {
    return name.trim();
  }
}
