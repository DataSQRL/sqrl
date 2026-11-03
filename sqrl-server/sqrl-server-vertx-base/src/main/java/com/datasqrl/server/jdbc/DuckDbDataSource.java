/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.server.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.duckdb.DuckDBConnection;

/**
 * Serves every connection as a {@link DuckDBConnection#duplicate()} of one physical connection.
 *
 * <p>Each physical {@code jdbc:duckdb:} connection opens a separate in-memory database with its own
 * settings, secrets and {@code memory_limit}. Hikari keeps exactly one physical connection, which
 * runs the init SQL once; duplicates of it share that database and can run queries concurrently.
 */
public final class DuckDbDataSource implements DataSource, AutoCloseable {

  private final HikariDataSource hikari;
  private final ReentrantLock lock = new ReentrantLock();
  private Connection root;
  private boolean closed;

  public DuckDbDataSource(String jdbcUrl, Optional<String> initSql) {
    var config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    config.setPoolName("duckdb-root");
    config.setMaximumPoolSize(1);
    config.setMinimumIdle(1);
    config.setMaxLifetime(0);
    initSql.ifPresent(config::setConnectionInitSql);
    this.hikari = new HikariDataSource(config);
  }

  @Override
  public Connection getConnection() throws SQLException {
    lock.lock();
    try {
      if (closed) {
        throw new SQLException("DuckDB data source is closed");
      }
      if (root == null) {
        // Borrowed for the lifetime of this data source and never returned to Hikari
        root = hikari.getConnection();
      }
      return root.unwrap(DuckDBConnection.class).duplicate();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return getConnection();
  }

  @Override
  public void close() throws SQLException {
    lock.lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      if (root != null) {
        root.close();
        root = null;
      }
    } finally {
      lock.unlock();
      hikari.close();
    }
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return hikari.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    hikari.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    hikari.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return hikari.getLoginTimeout();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return hikari.getParentLogger();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return hikari.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return hikari.isWrapperFor(iface);
  }
}
