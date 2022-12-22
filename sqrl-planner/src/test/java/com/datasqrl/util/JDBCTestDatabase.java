/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.engine.database.relational.JDBCEngineConfiguration;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.SneakyThrows;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

public class JDBCTestDatabase implements DatabaseHandle {

  public static final String TEST_DATABASE_NAME = "datasqrl";
  private final JDBCEngineConfiguration config;
  private final DatabaseEngine dbType;

  private PostgreSQLContainer postgreSQLContainer;

  public JDBCTestDatabase(IntegrationTestSettings.DatabaseEngine dbType) {
    this.dbType = dbType;
    if (dbType == DatabaseEngine.H2) {
      config = JDBCEngineConfiguration.builder()
          .config(JdbcDataSystemConnectorConfig.builder()
              //When the mem db is closed,
              .dbURL("jdbc:h2:mem:test_mem;DB_CLOSE_DELAY=-1")
              .driverName("org.h2.Driver")
              .dialect("h2")
              .database(TEST_DATABASE_NAME)
              .build())
          .build();
    } else if (dbType == DatabaseEngine.SQLITE) {
      config = JDBCEngineConfiguration.builder()
          .config(JdbcDataSystemConnectorConfig.builder()
              //A connection may be leaking somewhere, the inmem doesn't close after test is done
              .dbURL("jdbc:sqlite:file:test?mode=memory&cache=shared")
              .driverName("org.sqlite.JDBC")
              .dialect("sqlite")
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

  @SneakyThrows
  @Override
  public void cleanUp() {
    if (dbType == DatabaseEngine.H2) {
      try {
        //close after tests to clean up DB_CLOSE_DELAY
        DriverManager.getConnection(config.getConfig().getDbURL())
            .createStatement().execute("SHUTDOWN");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    if (postgreSQLContainer != null) {
      postgreSQLContainer.stop();
    }
  }
}
