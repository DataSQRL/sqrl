package com.datasqrl.engine.database.relational.ddl;

import java.util.Objects;
import lombok.EqualsAndHashCode;

public class PostgresCreateVectorExtensionStatement implements SqlDDLStatement {
  @Override
  public String toSql() {
    return "CREATE EXTENSION vector;";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PostgresCreateVectorExtensionStatement that = (PostgresCreateVectorExtensionStatement) o;
    return Objects.equals(toSql(), that.toSql());
  }

  @Override
  public int hashCode() {
    return Objects.hash(toSql());
  }
}
