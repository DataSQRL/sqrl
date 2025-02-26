/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import java.util.Set;

public class Clickstream extends UseCaseExample {

  public static final Clickstream INSTANCE = new Clickstream();

  protected Clickstream() {
    super(
        Set.of("click", "content"),
        script("clickstream-teaser", "trending", "visitafter", "recommendation"));
  }
}
