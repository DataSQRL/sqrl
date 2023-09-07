/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import java.util.Set;

public class Conference extends UseCaseExample {

  public static final Conference INSTANCE = new Conference();

  protected Conference() {
    super(Set.of("authtokens","events","emailtemplates"), scripts()
        .add("app", "events")
        .build());
  }

}
