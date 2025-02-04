package com.datasqrl.functions.vector;

import static com.datasqrl.function.FlinkUdfNsObject.getFunctionNameFromClass;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.sql.DatabaseExtension;
import com.datasqrl.vector.FlinkVectorType;
import com.datasqrl.vector.VectorFunctions;
import com.google.auto.service.AutoService;

import java.util.Set;
import java.util.stream.Collectors;

@AutoService(DatabaseExtension.class)
public class VectorPgExtension implements DatabaseExtension {
  public static final String ddlStatement = "CREATE EXTENSION IF NOT EXISTS vector";

  @Override
  public Class typeClass() {
    return FlinkVectorType.class;
  }

  @Override
  public Set<Name> operators() {
    return VectorFunctions.functions.stream()
        .map(f->getFunctionNameFromClass(f.getClass()))
        .collect(Collectors.toSet());
  }

  @Override
  public String getExtensionDdl() {
    return ddlStatement;
  }
}
