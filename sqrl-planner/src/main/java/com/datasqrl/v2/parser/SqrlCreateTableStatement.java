package com.datasqrl.v2.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;

import lombok.Value;

/**
 * Reprensets a CREATE TABLE statement
 */
@Value
public class SqrlCreateTableStatement implements SqrlDdlStatement {

  ParsedObject<String> createTable;
  SqrlComments comments;

  public String toSql() {
    return createTable.get();
  }

  @Override
  public FileLocation mapSqlLocation(FileLocation location) {
    return createTable.getFileLocation().add(location);
  }
}
