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
import com.datasqrl.util.DuckDbExtensions;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
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
  public Map<DatabaseType, SqlClient> createClients() {
    var clientsMapBuilder = ImmutableMap.<DatabaseType, SqlClient>builder();

    // PostgreSQL is always required and initializes synchronously
    clientsMapBuilder.put(DatabaseType.POSTGRES, initPostgresSqlClient());

    // DuckDB initializes synchronously
    initDuckdbSqlClient().ifPresent(client -> clientsMapBuilder.put(DatabaseType.DUCKDB, client));

    // Snowflake initializes synchronously
    initSnowflakeClient()
        .ifPresent(client -> clientsMapBuilder.put(DatabaseType.SNOWFLAKE, client));

    return clientsMapBuilder.build();
  }

  @SneakyThrows
  private Optional<SqlClient> initSnowflakeClient() {
    var snowflakeConf = config.getSnowflakeConfig();
    if (snowflakeConf == null) {
      return Optional.empty();
    }

    // No need for Class.forName() - Snowflake JDBC driver auto-registers via JDBC 4.0+
    // ServiceLoader
    var url = snowflakeConf.getUrl();
    url += "?CLIENT_SESSION_KEEP_ALIVE=true";

    return Optional.of(
        JDBCPool.pool(
            vertx,
            new JDBCConnectOptions().setJdbcUrl(url).setMetricsName("snowflake"),
            new PoolOptions(this.config.getPoolOptions()).setName("snowflake-pool")));
  }

  @SneakyThrows
  private Optional<SqlClient> initDuckdbSqlClient() {
    var duckDbConf = config.getDuckDbConfig();
    if (duckDbConf == null) {
      return Optional.empty();
    }

    // No need for Class.forName() - DuckDB JDBC driver auto-registers via JDBC 4.0+ ServiceLoader
    var url = duckDbConf.getUrl();
    var extensions = new DuckDbExtensions(duckDbConf.getConfig());
    var initSql = extensions.buildInitSql();

    var connectOptions =
        new JDBCConnectOptions()
            .setJdbcUrl(url)
            .setMetricsName("duckdb")
            .setExtraConfig(new JsonObject().put("initialSql", initSql));

    var poolOptions =
        new PoolOptions(this.config.getPoolOptions())
            .setName("duckdb-pool")
            .setMaxSize(1)
            .setMaxLifetime(0)
            .setIdleTimeout(0);

    return Optional.of(JDBCPool.pool(vertx, connectOptions, poolOptions));
  }

  private SqlClient initPostgresSqlClient() {
    var poolOptions = new PoolOptions(this.config.getPoolOptions()).setName("postgres-pool");
    // Note: setPipelined() method was removed in Vert.x 5, pipelining is now always enabled
    return Pool.pool(vertx, this.config.getPgConnectOptions(), poolOptions);
  }
}
