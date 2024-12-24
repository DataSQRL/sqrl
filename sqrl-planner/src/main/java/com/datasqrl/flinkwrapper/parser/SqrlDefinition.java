package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.flinkwrapper.SqrlEnvironment;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.table.api.SqlParserException;

/**
 * A partially parsed SQRL definition. Some elements are extracted but the
 * body of the definition is kept as a string to be passed to the Flink parser
 * for parsing and conversion.
 *
 * As such, we try to do our best to keep offsets so we can map errors back.
 */
@AllArgsConstructor
public abstract class SqrlDefinition implements SqrlStatement {

  ParsedObject<NamePath> tableName;
  ParsedObject<String> definitionBody;
  SqrlComments comments;

  @Override
  public void apply(SqrlEnvironment sqrlEnv) {
    String prefix = String.format("CREATE TEMPORARY VIEW %s AS ", tableName.get().toString());
    try {
      sqrlEnv.executeSQL(prefix + definitionBody.get());
    } catch (Exception e) {
      throw StatementParserException.from(e, definitionBody.getFileLocation(), prefix.length());
    }
  }

}
