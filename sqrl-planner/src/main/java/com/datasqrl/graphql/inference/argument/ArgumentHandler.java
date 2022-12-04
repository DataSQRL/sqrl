/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference.argument;

import com.datasqrl.graphql.inference.ArgumentSet;
import java.util.Set;

public interface ArgumentHandler {

  //Eventually public api
  public Set<ArgumentSet> accept(ArgumentHandlerContextV1 context);

  boolean canHandle(ArgumentHandlerContextV1 contextV1);
}