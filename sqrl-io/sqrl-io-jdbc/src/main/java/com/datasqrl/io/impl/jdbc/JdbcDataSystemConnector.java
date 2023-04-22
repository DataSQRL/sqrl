/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.jdbc;

import com.datasqrl.config.Constraints.Default;
import com.datasqrl.config.Constraints.MinLength;
import com.datasqrl.config.Constraints.Regex;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.util.constraints.OptionalMinString;
import com.fasterxml.jackson.annotation.JsonIgnore;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class JdbcDataSystemConnector implements DataSystemConnector {

  @Regex(match = "^[a-z][_a-z0-9$]{2,}$")
  String dbURL;
  @MinLength(min = 2)
  String dialect;
  @MinLength(min = 1)
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
  String driverName = null;

  @Override
  public boolean hasSourceTimestamp() {
    return false;
  }

}
