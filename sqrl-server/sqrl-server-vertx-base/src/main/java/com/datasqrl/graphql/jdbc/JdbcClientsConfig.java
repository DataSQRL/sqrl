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
package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.config.ServerConfig;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration class responsible for creating and managing database clients for different database
 * types (PostgreSQL, DuckDB, Snowflake).
 */
@RequiredArgsConstructor
@Slf4j
public class JdbcClientsConfig {

  private final Vertx vertx;
  private final ServerConfig config;

  /** Creates a map of database clients for all configured database types. */
  public Future<Map<DatabaseType, SqlClientWrapper>> createClients() {
    var clientsMapBuilder = ImmutableMap.<DatabaseType, SqlClientWrapper>builder();
    var initFutures = new ArrayList<Future<SqlClientWrapper>>();

    // PostgreSQL is always required and initializes synchronously
    clientsMapBuilder.put(DatabaseType.POSTGRES, initPostgresSqlClient());

    // DuckDB initialization is async (needs to install extensions)
    initDuckdbSqlClient()
        .ifPresent(
            wrapperFuture -> {
              var mappedFuture =
                  wrapperFuture.onSuccess(
                      wrapper -> clientsMapBuilder.put(DatabaseType.DUCKDB, wrapper));
              initFutures.add(mappedFuture);
            });

    // Snowflake initializes synchronously
    initSnowflakeClient()
        .ifPresent(client -> clientsMapBuilder.put(DatabaseType.SNOWFLAKE, client));

    if (initFutures.isEmpty()) {
      return Future.succeededFuture(clientsMapBuilder.build());
    }

    return Future.all(initFutures).map(v -> clientsMapBuilder.build());
  }

  @SneakyThrows
  private Optional<SqlClientWrapper> initSnowflakeClient() {
    var snowflakeConf = config.getSnowflakeConfig();
    if (snowflakeConf == null) {
      return Optional.empty();
    }

    // No need for Class.forName() - Snowflake JDBC driver auto-registers via JDBC 4.0+
    // ServiceLoader
    var url = snowflakeConf.getUrl();
    url += "?CLIENT_SESSION_KEEP_ALIVE=true";
    var pool = initJdbcPool(url, "snowflake-pool", "snowflake");

    return Optional.of(new SqlClientWrapper(pool));
  }

  @SneakyThrows
  private Optional<Future<SqlClientWrapper>> initDuckdbSqlClient() {
    var duckDbConf = config.getDuckDbConfig();
    if (duckDbConf == null) {
      return Optional.empty();
    }

    // No need for Class.forName() - DuckDB JDBC driver auto-registers via JDBC 4.0+ ServiceLoader
    var url = duckDbConf.getUrl();
    var pool = initJdbcPool(url, "duckdb-pool", "duckdb");
    var wrapper = new SqlClientWrapper.DuckDbClientWrapper(pool, duckDbConf.getConfig());

    // Install extensions asynchronously (they persist in the database file)
    Future<SqlClientWrapper> wrapperFuture =
        pool.query(wrapper.getExtensionInstall())
            .execute()
            .map(
                v -> {
                  log.info("DuckDB extensions installed successfully");
                  return (SqlClientWrapper) wrapper.withClient(pool);
                })
            .recover(
                err -> {
                  log.warn(
                      "Failed to install DuckDB extensions (may already be installed): {}",
                      err.getMessage());
                  return Future.succeededFuture(wrapper);
                });

    return Optional.of(wrapperFuture);
  }

  private SqlClientWrapper initPostgresSqlClient() {
    var poolOptions = new PoolOptions(this.config.getPoolOptions()).setName("postgres-pool");
    // Note: setPipelined() method was removed in Vert.x 5, pipelining is now always enabled
    var pool = Pool.pool(vertx, this.config.getPgConnectOptions(), poolOptions);

    return new SqlClientWrapper(pool);
  }

  @SneakyThrows
  private Pool initJdbcPool(String url, String poolName, String metricsName) {
    var connectOptions = new JDBCConnectOptions().setJdbcUrl(url).setMetricsName(metricsName);
    var poolOptions = new PoolOptions(this.config.getPoolOptions()).setName(poolName);

    return JDBCPool.pool(vertx, connectOptions, poolOptions);
  }
}
