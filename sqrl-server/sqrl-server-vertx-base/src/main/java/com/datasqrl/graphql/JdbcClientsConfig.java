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
package com.datasqrl.graphql;

import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.google.common.base.Strings;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBDriver;

/**
 * Configuration class responsible for creating and managing database clients for different database
 * types (PostgreSQL, DuckDB, Snowflake).
 */
@Slf4j
public class JdbcClientsConfig {

  private final Vertx vertx;
  private final ServerConfig config;
  private final Optional<String> snowflakeUrl;

  public JdbcClientsConfig(Vertx vertx, ServerConfig config) {
    this.vertx = vertx;
    this.config = config;
    this.snowflakeUrl = readSnowflakeUrl();
  }

  /** Creates a map of database clients for all configured database types. */
  public Map<DatabaseType, SqlClient> createClients() {
    var clients = new HashMap<DatabaseType, SqlClient>();
    clients.put(DatabaseType.POSTGRES, createPostgresSqlClient());
    clients.put(DatabaseType.DUCKDB, createDuckdbSqlClient());
    snowflakeUrl.ifPresent(url -> clients.put(DatabaseType.SNOWFLAKE, createSnowflakeClient(url)));
    return clients;
  }

  @SneakyThrows
  private static Optional<String> readSnowflakeUrl() {
    File snowflakeConfig = new File("snowflake-config.json");
    Map map = null;
    if (snowflakeConfig.exists()) {
      map = HttpServerVerticle.getObjectMapper().readValue(snowflakeConfig, Map.class);
      if (map.isEmpty()) {
        return Optional.empty();
      }
    } else {
      return Optional.empty();
    }

    var url = (String) map.get("url");
    if (Strings.isNullOrEmpty(url)) {
      log.warn("Url must be specified in the snowflake engine");
      return Optional.empty();
    }
    return Optional.of(url);
  }

  @SneakyThrows
  private SqlClient createSnowflakeClient(String url) {
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

    var connectOptions =
        new JDBCConnectOptions().setJdbcUrl(url + "?CLIENT_SESSION_KEEP_ALIVE=true");

    return JDBCPool.pool(vertx, connectOptions, new PoolOptions());
  }

  @SneakyThrows
  private SqlClient createDuckdbSqlClient() {
    // In-memory DuckDB instance or you can specify a file path for persistence
    var url = "jdbc:duckdb:";

    Class.forName("org.duckdb.DuckDBDriver");

    var connectOptions =
        new JDBCConnectOptions().setJdbcUrl(url + "?" + DuckDBDriver.JDBC_STREAM_RESULTS + "=true");

    return JDBCPool.pool(vertx, connectOptions, new PoolOptions());
  }

  private SqlClient createPostgresSqlClient() {
    var poolOptions = new PoolOptions(this.config.getPoolOptions());
    // Note: setPipelined() method was removed in Vert.x 5, pipelining is now always enabled
    return Pool.pool(vertx, this.config.getPgConnectOptions(), poolOptions);
  }
}
