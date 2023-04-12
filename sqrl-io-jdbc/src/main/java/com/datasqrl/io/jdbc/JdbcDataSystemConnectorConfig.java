/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.jdbc;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.util.constraints.OptionalMinString;
import com.google.auto.service.AutoService;
import lombok.*;

import javax.validation.constraints.NotNull;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@AutoService(DataSystemConnectorConfig.class)
public class JdbcDataSystemConnectorConfig implements DataSystemConnectorConfig {

  public static final String SYSTEM_TYPE = "jdbc";

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
    return SYSTEM_TYPE;
  }
}
