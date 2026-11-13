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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.duckdb.DuckDBConnection;

/**
 * Serves every connection as a {@link DuckDBConnection#duplicate()} of one root connection.
 *
 * <p>Each {@code jdbc:duckdb:} connection opened through the driver is a separate in-memory
 * database with its own settings, secrets and {@code memory_limit}. The constructor opens the root
 * connection and runs the init SQL on it, so a failing init SQL fails server start. Duplicates
 * share the root's database and can run queries concurrently.
 */
public final class DuckDbDataSource implements DataSource, AutoCloseable {

  private final DuckDBConnection root;
  private final ReentrantLock lock = new ReentrantLock();
  private boolean closed;

  public DuckDbDataSource(String jdbcUrl, String initSql) throws SQLException {
    root = DriverManager.getConnection(jdbcUrl).unwrap(DuckDBConnection.class);
    try (var stmt = root.createStatement()) {
      stmt.execute(initSql);
    } catch (SQLException e) {
      root.close();
      throw e;
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    lock.lock();
    try {
      if (closed) {
        throw new SQLException("DuckDB data source is closed");
      }
      return root.duplicate();
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
      if (!closed) {
        closed = true;
        root.close();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public PrintWriter getLogWriter() {
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) {}

  @Override
  public void setLoginTimeout(int seconds) {}

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException("Not a wrapper for " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }
}
