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
package com.datasqrl.graphql.config;

import com.datasqrl.graphql.jdbc.DatabaseType;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Database configuration for the Spring Boot GraphQL server. Sets up R2DBC for PostgreSQL (async)
 * and JDBC for DuckDB/Snowflake.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class DatabaseConfiguration {

  private final ServerConfigProperties config;

  @Bean
  @Primary
  public ConnectionFactory postgresConnectionFactory() {
    var pgConfig = config.getPostgres();

    var connectionConfig =
        PostgresqlConnectionConfiguration.builder()
            .host(pgConfig.getHost())
            .port(pgConfig.getPort())
            .database(pgConfig.getDatabase())
            .username(pgConfig.getUser())
            .password(pgConfig.getPassword())
            .build();

    var connectionFactory = new PostgresqlConnectionFactory(connectionConfig);

    var poolConfig =
        ConnectionPoolConfiguration.builder(connectionFactory)
            .maxSize(pgConfig.getPoolSize())
            .maxIdleTime(Duration.ofMillis(pgConfig.getMaxIdleTime()))
            .build();

    log.info(
        "Creating R2DBC PostgreSQL connection pool: {}:{}/{}",
        pgConfig.getHost(),
        pgConfig.getPort(),
        pgConfig.getDatabase());

    return new ConnectionPool(poolConfig);
  }

  @Bean(name = "duckDbDataSource")
  @ConditionalOnProperty(prefix = "sqrl.server.duck-db", name = "path")
  public DataSource duckDbDataSource() {
    var duckDbConfig = config.getDuckDb();
    if (duckDbConfig == null) {
      return null;
    }

    var hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl("jdbc:duckdb:" + duckDbConfig.getPath());
    hikariConfig.setMaximumPoolSize(duckDbConfig.getPoolSize());
    hikariConfig.setDriverClassName("org.duckdb.DuckDBDriver");

    log.info("Creating DuckDB connection pool: {}", duckDbConfig.getPath());

    return new HikariDataSource(hikariConfig);
  }

  @Bean(name = "snowflakeDataSource")
  @ConditionalOnProperty(prefix = "sqrl.server.snowflake", name = "url")
  public DataSource snowflakeDataSource() {
    var snowflakeConfig = config.getSnowflake();
    if (snowflakeConfig == null) {
      return null;
    }

    var hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(snowflakeConfig.getUrl());
    hikariConfig.setUsername(snowflakeConfig.getUser());
    hikariConfig.setPassword(snowflakeConfig.getPassword());
    hikariConfig.setDriverClassName("net.snowflake.client.jdbc.SnowflakeDriver");

    if (snowflakeConfig.getDatabase() != null) {
      hikariConfig.addDataSourceProperty("db", snowflakeConfig.getDatabase());
    }
    if (snowflakeConfig.getSchema() != null) {
      hikariConfig.addDataSourceProperty("schema", snowflakeConfig.getSchema());
    }
    if (snowflakeConfig.getWarehouse() != null) {
      hikariConfig.addDataSourceProperty("warehouse", snowflakeConfig.getWarehouse());
    }

    if (snowflakeConfig.getProperties() != null) {
      snowflakeConfig.getProperties().forEach(hikariConfig::addDataSourceProperty);
    }

    log.info("Creating Snowflake connection pool: {}", snowflakeConfig.getUrl());

    return new HikariDataSource(hikariConfig);
  }

  @Bean
  public Map<DatabaseType, Object> databaseClients(
      ConnectionFactory postgresConnectionFactory,
      @org.springframework.beans.factory.annotation.Autowired(required = false)
          @org.springframework.beans.factory.annotation.Qualifier("duckDbDataSource")
          DataSource duckDbDataSource,
      @org.springframework.beans.factory.annotation.Autowired(required = false)
          @org.springframework.beans.factory.annotation.Qualifier("snowflakeDataSource")
          DataSource snowflakeDataSource) {

    Map<DatabaseType, Object> clients = new EnumMap<>(DatabaseType.class);

    clients.put(DatabaseType.POSTGRES, postgresConnectionFactory);

    if (duckDbDataSource != null) {
      clients.put(DatabaseType.DUCKDB, duckDbDataSource);
    }

    if (snowflakeDataSource != null) {
      clients.put(DatabaseType.SNOWFLAKE, snowflakeDataSource);
    }

    log.info("Configured database clients: {}", clients.keySet());

    return clients;
  }
}
