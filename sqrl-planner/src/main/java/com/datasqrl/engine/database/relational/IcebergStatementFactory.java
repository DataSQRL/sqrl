package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.convert.RelToSqlNode;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan.Query;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;

public class IcebergStatementFactory extends AbstractJdbcStatementFactory {

  public IcebergStatementFactory() {
    super(null, null); //Iceberg does not support queries
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
