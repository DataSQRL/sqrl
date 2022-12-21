/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.engine.database.relational.JDBCEngineConfiguration;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.Getter;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

public class JDBCTestDatabase implements DatabaseHandle {

  public static final String TEST_DATABASE_NAME = "datasqrl";
  private final JDBCEngineConfiguration config;

  private PostgreSQLContainer postgreSQLContainer;

  public JDBCTestDatabase(IntegrationTestSettings.DatabaseEngine dbType) {
    if (dbType == DatabaseEngine.H2) {
      config = JDBCEngineConfiguration.builder()
          .config(JdbcDataSystemConnectorConfig.builder()
              .dbURL("jdbc:h2:mem:test_mem;DB_CLOSE_DELAY=-1")
              .driverName("org.h2.Driver")
              .dialect("h2")
              .database(TEST_DATABASE_NAME)
              .build())
          .build();
    } else if (dbType == IntegrationTestSettings.DatabaseEngine.POSTGRES) {
      DockerImageName image = DockerImageName.parse("postgres:14.2");
      postgreSQLContainer = new PostgreSQLContainer(image)
          .withDatabaseName(TEST_DATABASE_NAME);
      postgreSQLContainer.start();

      config = JDBCEngineConfiguration.builder()
          .config(JdbcDataSystemConnectorConfig.builder()
              .host(postgreSQLContainer.getHost())
              .port(postgreSQLContainer.getMappedPort(5432))
              .dbURL(postgreSQLContainer.getJdbcUrl())
              .driverName(postgreSQLContainer.getDriverClassName())
              .dialect("postgres")
              .user(postgreSQLContainer.getUsername())
              .password(postgreSQLContainer.getPassword())
              .database(TEST_DATABASE_NAME)
              .build())
          .build();
    } else {
      throw new UnsupportedOperationException("Not a supported db type: " + dbType);
    }
  }

  public JDBCEngineConfiguration getJdbcConfiguration() {
    return config;
  }

  @Override
  public void cleanUp() {
    if (postgreSQLContainer != null) {
      postgreSQLContainer.stop();
    }
  }
}
