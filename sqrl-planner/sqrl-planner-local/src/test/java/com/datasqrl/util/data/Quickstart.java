/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import java.util.Set;

public class Quickstart extends UseCaseExample {

  public static final Quickstart INSTANCE = new Quickstart();

  protected Quickstart() {
    super(Set.of("products", "orders", "customers"), script("quickstart-intro-local", "users", "orders", "totals", "spending"));
  }
}
