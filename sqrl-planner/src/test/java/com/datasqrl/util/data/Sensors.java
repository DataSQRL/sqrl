/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.util.data;

import java.util.Set;

public class Sensors extends UseCaseExample {

  public static final Sensors INSTANCE = new Sensors("");

  public static final Sensors INSTANCE_EPOCH = new Sensors("epoch");
  public static final Sensors INSTANCE_MUTATION = new Sensors("mutation", "metricsapi.graphqls");

  protected Sensors(String variant) {
    super(
        variant,
        Set.of("sensors", "sensorreading", "machinegroup"),
        scripts()
            .add("sensors-teaser", "machine", "minreadings")
            .add("metrics-function", "secreading", "sensormaxtemp")
            .build());
  }

  protected Sensors(String variant, String graphql) {
    super(variant, Set.of("sensormaxtemp"), script("metrics", "sensormaxtemp"), graphql);
  }
}
