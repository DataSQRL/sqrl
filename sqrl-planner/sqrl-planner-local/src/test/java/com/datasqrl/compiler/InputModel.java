/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compiler;

import com.datasqrl.io.DataSystemConnectorConfig;
import lombok.Value;

public class InputModel {

  @Value
  public static class DataSource {

    String name;
    DataSystemConnectorConfig source;
  }
}
