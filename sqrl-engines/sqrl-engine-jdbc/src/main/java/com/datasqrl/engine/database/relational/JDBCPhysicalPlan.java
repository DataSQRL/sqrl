/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class JDBCPhysicalPlan implements DatabasePhysicalPlan {

  List<SqlDDLStatement> ddlStatements;
  Map<IdentifiedQuery, QueryTemplate> queries;
  Map<String, String> queryStrings;

  @Override
  public void writeTo(Path deployDir, String stageName, Deserializer serializer)
      throws IOException {
    Files.writeString(deployDir.resolve(getSchemaFilename(stageName)), createDDL());
    if (!queryStrings.isEmpty()) {
      Path queryDir = deployDir.resolve(stageName + QUERY_FOLDER_SUFFIX);
      if (!Files.isDirectory(queryDir)) {
        Files.createDirectories(queryDir);
      }
      for (Map.Entry<String, String> queryEntry : queryStrings.entrySet()) {
        Files.writeString(queryDir.resolve(queryEntry.getKey() + SQL_EXTENSION), queryEntry.getValue());
      }
    }
  }

  private String createDDL() {
    return ddlStatements.stream()
        .map(SqlDDLStatement::toSql)
        .collect(Collectors.joining("\n"));
  }

  private static final String SQL_EXTENSION = ".sql";

  private static final String SCHEMA_FILENAME_SUFFIX = "-schema" + SQL_EXTENSION;

  private static final String QUERY_FOLDER_SUFFIX = "-queries";

  public static String getSchemaFilename(String stageName) {
    return stageName+SCHEMA_FILENAME_SUFFIX;
  }
}
