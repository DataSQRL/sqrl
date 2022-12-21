/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.jdbc;

import com.datasqrl.io.DataSystemConnector;

public class JdbcDataSystemConnector implements DataSystemConnector {

  @Override
  public boolean hasSourceTimestamp() {
    return false;
  }

  @Override
  public String getPrefix() {
    return "jdbc";
  }
}
