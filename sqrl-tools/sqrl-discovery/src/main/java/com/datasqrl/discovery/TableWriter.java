/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.schema.input.external.SchemaExport;
import com.datasqrl.model.schema.TableDefinition;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import lombok.NonNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class TableWriter {
  public static final String TABLE_FILE_SUFFIX = ".table.json";

  private final ObjectMapper jsonMapper;
  private final YAMLMapper yamlMapper;
  SchemaExport export = new SchemaExport();


  public TableWriter() {
    this.jsonMapper = SqrlObjectMapper.INSTANCE;
    this.yamlMapper = new YAMLMapper();
    this.yamlMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  public void writeToFile(@NonNull Path destinationDir, @NonNull List<TableSource> tables)
      throws IOException {
    for (TableSource table : tables) {
      writeToFile(destinationDir, table);
    }
    if (tables.isEmpty()) {
      throw new RuntimeException("Discovery found no tables.");
    }
  }

  public void writeToFile(@NonNull Path destinationDir, @NonNull TableSource table) throws IOException  {
    Path tableConfigFile = destinationDir.resolve(
            table.getName().getCanonical() + TABLE_FILE_SUFFIX);
    TableSchema ts = table.getSchema().getSchema();
    TableConfig config = table.getConfiguration().toBuilder().schema(ts.getSchemaType()).build();
    config.toFile(tableConfigFile);
    if (ts instanceof FlexibleTableSchema) {
      TableDefinition outputSchema = export.export((FlexibleTableSchema) ts);
      Path schemaFile = destinationDir.resolve(FlexibleTableSchemaFactory.getSchemaFilename(config));
      yamlMapper.writeValue(schemaFile.toFile(), outputSchema);
    }
  }

}
