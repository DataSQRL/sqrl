/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.NonNull;

public class TableWriter {
  public static final String TABLE_FILE_SUFFIX = ".table.json";

  public TableWriter() {

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
    TableSchema ts = table.getSchema();
    TableConfig config = table.getConfiguration().toBuilder().schema(ts.getSchemaType()).build();
    config.toFile(tableConfigFile);
    TableSchemaFactory schemaFactory = TableSchemaFactory.load(ts.getSchemaType());
    Path schemaFile = destinationDir.resolve(schemaFactory.getSchemaFilename(config));
    Files.writeString(schemaFile, ts.getDefinition());
  }

}
