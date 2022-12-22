/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.jdbc;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.util.constraints.OptionalMinString;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JdbcDataSystemConnectorConfig implements DataSystemConnectorConfig {

  String host;
  Integer port;
  @NonNull @NotNull
  String dbURL;
  String user;
  String password;
  @OptionalMinString
  String driverName;
  @NonNull @NotNull
  String dialect;
  @NonNull @NotNull
  String database;

  @Override
  public DataSystemConnector initialize(@NonNull ErrorCollector errors) {
    return new JdbcDataSystemConnector();
  }

  @Override
  public String getSystemType() {
    return "jdbc";
  }
}
