package com.datasqrl.engine.database.relational;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.OperatorRuleTransformer;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.plan.global.IndexDefinition;

public class IcebergStatementFactory extends AbstractJdbcStatementFactory {

  public IcebergStatementFactory() {
    super(new OperatorRuleTransformer(Dialect.POSTGRES), null, null); //Iceberg does not support queries
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type) {
    //TODO: Need to create Iceberg cast spec
    return ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(type);
  }

  @Override
  public boolean supportsQueries() {
    return false;
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("Iceberg does not support indexes");
  }
}
