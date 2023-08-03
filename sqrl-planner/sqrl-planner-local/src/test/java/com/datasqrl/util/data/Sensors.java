/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import java.util.Set;


public class Sensors extends UseCaseExample {

  public static final Sensors INSTANCE = new Sensors("");

  public static final Sensors INSTANCE_EPOCH = new Sensors("epoch");
  public static final Sensors INSTANCE_MUTATION
      = new Sensors("mutation", "metricsapi.graphqls");


  protected Sensors(String variant) {
    super(variant, Set.of("sensors","sensorreading","machinegroup"),
        scripts().add("sensors-teaser","machine","minreadings")
            .add("metrics-function", "secreading", "sensormaxtemp").build());
  }

  protected Sensors(String variant, String graphql) {
    super(variant, Set.of("sensormaxtemp"),
        script("metrics","sensormaxtemp"),
        graphql);
  }
}
