/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import java.util.Set;

public class Repository extends UseCaseExample {

  public static final Repository INSTANCE = new Repository();

  protected Repository() {
    super(Set.of("package"), script("repo", "package", "submission"));
  }
}
