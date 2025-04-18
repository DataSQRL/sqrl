package com.datasqrl.graphql.jdbc;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum DatabaseType {

  NONE(false, false),
  POSTGRES(true, true),
  DUCKDB(true, true),
  SNOWFLAKE(false, false);

  public final boolean supportsLimitOffsetBinding;
  public final boolean supportsPositionalParameters;

  public String getNormalizedName() {
    return name().toLowerCase();
  }

}
