/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.jdbc;

import com.datasqrl.config.Constraints.Default;
import com.datasqrl.config.Constraints.MinLength;
import com.datasqrl.config.Constraints.Regex;
import com.datasqrl.io.DataSystemConnector;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class JdbcDataSystemConnector implements DataSystemConnector, Serializable {

  @MinLength(min = 3)
  String url;
  @MinLength(min = 2)
  String dialect;
  @Regex(match = "^[a-z][_a-z0-9$]{2,}$")
  String database;
  @Default
  String host = null;
  @Default
  Integer port = null;
  @Default
  String user = null;
  @Default
  String password = null;
  @Default @MinLength(min = 3)
  String driver = null;

  @Override
  public boolean hasSourceTimestamp() {
    return false;
  }

}
