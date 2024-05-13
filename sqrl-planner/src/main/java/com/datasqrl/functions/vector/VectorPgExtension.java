package com.datasqrl.functions.vector;

import static com.datasqrl.function.FlinkUdfNsObject.getFunctionNameFromClass;

import com.datasqrl.sql.PgExtension;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.vector.FlinkVectorType;
import com.datasqrl.vector.VectorFunctions;
import com.google.auto.service.AutoService;

import java.util.Set;
import java.util.stream.Collectors;

@AutoService(PgExtension.class)
public class VectorPgExtension implements PgExtension {
  public final SqlDDLStatement ddlStatement = () -> "CREATE EXTENSION IF NOT EXISTS vector;";

  @Override
  public Class typeClass() {
    return FlinkVectorType.class;
  }

  @Override
  public Set<String> operators() {
    return VectorFunctions.functions.stream()
        .map(f->getFunctionNameFromClass(f.getClass()).getDisplay())
        .collect(Collectors.toSet());
  }

  @Override
  public SqlDDLStatement getExtensionDdl() {
    return ddlStatement;
  }
}
