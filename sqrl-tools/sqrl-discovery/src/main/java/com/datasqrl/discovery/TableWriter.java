/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import com.datasqrl.planner.tables.FlinkTableBuilder;

import lombok.NonNull;

public class TableWriter {
  public static final String TABLE_FILE_SUFFIX = ".table.sql";

  public TableWriter() {

  }

  public Collection<Path> writeToFile(@NonNull Path destinationDir, @NonNull FlinkTableBuilder table) throws IOException  {
    var tableConfigFile = destinationDir.resolve(
        table.getTableName() + TABLE_FILE_SUFFIX);
    var sql = table.buildSql(false).toString() + ";";
    Files.writeString(tableConfigFile, sql);
    return List.of(tableConfigFile);
  }

}
