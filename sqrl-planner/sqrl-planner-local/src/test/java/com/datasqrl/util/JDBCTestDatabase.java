/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import lombok.Getter;
import lombok.SneakyThrows;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class JDBCTestDatabase implements DatabaseHandle {

  public static final String TEST_DATABASE_NAME = "datasqrl";
  private final JdbcDataSystemConnector connector;
  private final DatabaseEngine dbType;
  private Connection sqliteConn;

  @Getter
  private PostgreSQLContainer postgreSQLContainer;

  @SneakyThrows
  public JDBCTestDatabase(IntegrationTestSettings.DatabaseEngine dbType) {
    this.dbType = dbType;
    if (dbType == DatabaseEngine.H2) {
      connector = JdbcDataSystemConnector.builder()
              //When the mem db is closed,
              .url("jdbc:h2:mem:test_mem;DB_CLOSE_DELAY=-1")
              .driver("org.h2.Driver")
              .dialect("h2")
              .database(TEST_DATABASE_NAME)
              .build();
    } else if (dbType == DatabaseEngine.SQLITE) {
      connector = JdbcDataSystemConnector.builder()
              //A connection may be leaking somewhere, the inmem doesn't close after test is done
              .url("jdbc:sqlite:file:test?mode=memory&cache=shared")
              .driver("org.sqlite.JDBC")
              .dialect("sqlite")
              .database(TEST_DATABASE_NAME)
              .build();
      //Hold open the connection so the db stays around
      this.sqliteConn = DriverManager.getConnection(connector.getUrl());
    } else if (dbType == IntegrationTestSettings.DatabaseEngine.POSTGRES) {
      postgreSQLContainer = new PostgreSQLContainer(
          DockerImageName.parse("ankane/pgvector:v0.5.0")
              .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName(TEST_DATABASE_NAME);
      postgreSQLContainer.start();

      connector = JdbcDataSystemConnector.builder()
              .host(postgreSQLContainer.getHost())
              .port(postgreSQLContainer.getMappedPort(5432))
              .url(postgreSQLContainer.getJdbcUrl())
              .driver(postgreSQLContainer.getDriverClassName())
              .dialect("postgres")
              .user(postgreSQLContainer.getUsername())
              .password(postgreSQLContainer.getPassword())
              .database(TEST_DATABASE_NAME)
              .build();
    } else {
      throw new UnsupportedOperationException("Not a supported db type: " + dbType);
    }
  }

  public JdbcDataSystemConnector getConnector() {
    return connector;
  }

  @SneakyThrows
  @Override
  public void cleanUp() {
    if (dbType == DatabaseEngine.SQLITE && sqliteConn != null) {
      sqliteConn.close();
    }
    if (dbType == DatabaseEngine.H2) {
      try {
        //close after tests to clean up DB_CLOSE_DELAY
        DriverManager.getConnection(connector.getUrl())
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
