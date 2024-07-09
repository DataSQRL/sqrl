/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import java.util.Set;


public class Banking extends UseCaseExample {

  public static final Banking INSTANCE = new Banking();

  protected Banking() {
    super("", Set.of("customers","applications", "application_updates", "loan_types"),
        script("loan","overview","applicationupdates"),
        "schema.graphqls");
  }
}
