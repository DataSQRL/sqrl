package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLocation.FileLocation;

public class SqrlDistinctStatement extends SqrlDefinition {

  public SqrlDistinctStatement(
      ParsedObject<NamePath> tableName,
      SqrlComments comments,
      ParsedObject<NamePath> from,
      ParsedObject<String> columns,
      ParsedObject<String> remaining) {
    super(tableName,
        new ParsedObject<>(String.format("SELECT %s FROM (SELECT * FROM %s ORDER BY %s)",
                                          columns.get(), from.get(), remaining.get()),
            columns.getFileLocation()),
        comments);
  }

}
