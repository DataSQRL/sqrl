/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config.provider;

import java.sql.Connection;
import java.sql.SQLException;
import lombok.NonNull;

public interface JDBCConnectionProvider extends DatabaseConnectionProvider {

  String getHost();

  int getPort();

  @NonNull String getDbURL();

  String getUser();

  String getPassword();

  String getDriverName();

  @NonNull String getDatabaseName();

  Dialect getDialect();

  Connection getConnection() throws SQLException, ClassNotFoundException;
}
