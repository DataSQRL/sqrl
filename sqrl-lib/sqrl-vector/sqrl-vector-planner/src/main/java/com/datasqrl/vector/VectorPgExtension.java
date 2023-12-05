package com.datasqrl.vector;

import com.datasqrl.sql.PgExtension;
import com.datasqrl.function.SqrlFunction;
import com.datasqrl.sql.SqlDDLStatement;
import com.google.auto.service.AutoService;

import java.util.Set;

@AutoService(PgExtension.class)
public class VectorPgExtension implements PgExtension {
  public final SqlDDLStatement ddlStatement = () -> "CREATE EXTENSION IF NOT EXISTS vector;";

  @Override
  public Class typeClass() {
    return FlinkVectorType.class;
  }

  @Override
  public Set<SqrlFunction> operators() {
    return VectorFunctions.functions;
  }

  @Override
  public SqlDDLStatement getExtensionDdl() {
    return ddlStatement;
  }
}
