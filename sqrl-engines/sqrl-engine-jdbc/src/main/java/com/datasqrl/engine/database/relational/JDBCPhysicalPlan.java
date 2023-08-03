/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.PhysicalPlan.StagePlan;
import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
public class JDBCPhysicalPlan implements DatabasePhysicalPlan {

  List<SqlDDLStatement> ddlStatements;
  Map<IdentifiedQuery, QueryTemplate> queries;

  @Override
  public void writeTo(Path deployDir, String stageName, Deserializer serializer)
      throws IOException {
    Files.writeString(deployDir.resolve(getSchemaFilename(stageName)), createDDL());
  }

  private String createDDL() {
    return ddlStatements.stream()
        .map(SqlDDLStatement::toSql)
        .collect(Collectors.joining("\n"));
  }

  private static final String SCHEMA_FILENAME_SUFFIX = "-schema.sql";

  public static String getSchemaFilename(String stageName) {
    return stageName+SCHEMA_FILENAME_SUFFIX;
  }
}
