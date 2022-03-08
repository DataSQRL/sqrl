package org.apache.calcite.sql.type;

import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

/**
 * Types are interned so type inference may return types with incorrect multiplicity. Only the
 * multiplicity on non-inferred columns should be considered, such as on TableScan.
 */
public class SqrlBasicSqlType extends BasicSqlType {
  private final Multiplicity multiplicity;

  public SqrlBasicSqlType(SqlTypeName typeName, boolean isNullable,
      Multiplicity multiplicity) {
    super(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM, typeName);
    this.isNullable = isNullable;
    this.multiplicity = multiplicity;
    computeDigest();
  }

  @Override
  public boolean isNullable() {
    return isNullable;
  }

  public Multiplicity getMultiplicity() {
    return multiplicity;
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(this.typeName.name());
  }
}
