package com.datasqrl.config;

import com.datasqrl.graphql.jdbc.DatabaseType;
import java.util.Arrays;
import java.util.Optional;

public enum JdbcDialect {

  Postgres("PostgreSQL"), Oracle, MySQL, SQLServer, H2, SQLite, Iceberg, Snowflake, DuckDB;

  private final String[] synonyms;

  private JdbcDialect(String... synonyms) {
    this.synonyms = synonyms;
  }

  private boolean matches(String dialect) {
    if (name().equalsIgnoreCase(dialect)) return true;
    for (String synonym : synonyms) {
      if (synonym.equalsIgnoreCase(dialect)) return true;
    }
    return false;
  }

  public String getId() {
    return name().toLowerCase();
  }

  public static Optional<JdbcDialect> find(String dialect) {
    return Arrays.stream(values()).filter(d -> d.matches(dialect)).findFirst();
  }

  public DatabaseType getDatabaseType() {
    switch (this) {
      case Postgres: return DatabaseType.POSTGRES;
      case DuckDB: return DatabaseType.DUCKDB;
      case Snowflake: return DatabaseType.SNOWFLAKE;
      default: throw new UnsupportedOperationException("Dialect not yet supported by server: " + this);
    }
  }

}
