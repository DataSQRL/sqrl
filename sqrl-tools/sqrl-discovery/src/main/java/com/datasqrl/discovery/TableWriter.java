/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import com.datasqrl.config.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import lombok.NonNull;

public class TableWriter {
  public static final String TABLE_FILE_SUFFIX = ".table.sql";
  public static final String TABLE_FILE_SUFFIX_DEPRECATED = ".table.json";

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

  public Collection<Path> writeToFile(@NonNull Path destinationDir, @NonNull TableSource table) throws IOException  {
    Path tableConfigFile = destinationDir.resolve(
            table.getName().getCanonical() + TABLE_FILE_SUFFIX_DEPRECATED);
    TableSchema ts = table.getTableSchema().get();
    TableConfig config = table.getConfiguration().toBuilder().build();
    config.toFile(tableConfigFile, true);
    TableSchemaFactory schemaFactory = TableSchemaFactory.loadByType(ts.getSchemaType());
    Path schemaFile = destinationDir.resolve(schemaFactory.getSchemaFilename(config));
    Files.writeString(schemaFile, ts.getDefinition());
    return List.of(tableConfigFile, schemaFile);
  }

  public Collection<Path> writeToFile(@NonNull Path destinationDir, @NonNull FlinkTableBuilder table) throws IOException  {
    Path tableConfigFile = destinationDir.resolve(
        table.getTableName() + TABLE_FILE_SUFFIX);
    String sql = table.buildSql(false).toString() + ";";
    Files.writeString(tableConfigFile, sql);
    return List.of(tableConfigFile);
  }

}
