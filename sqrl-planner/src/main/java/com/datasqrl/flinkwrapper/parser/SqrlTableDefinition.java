package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.AllArgsConstructor;

/**
 * A partially parsed SQRL definition. Some elements are extracted but the
 * body of the definition is kept as a string to be passed to the Flink parser
 * for parsing and conversion.
 *
 * As such, we try to do our best to keep offsets so we can map errors back.
 */
public class SqrlTableDefinition extends SqrlDefinition implements StackableStatement {

  public SqrlTableDefinition(ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody, AccessModifier access,
      SqrlComments comments) {
    super(tableName, definitionBody, access, comments);
  }

  @Override
  public boolean isRoot() {
    return true;
  }

  public SqrlTableFunctionStatement toFunction() {
    return new SqrlTableFunctionStatement(tableName, definitionBody, access, comments,
        List.of(), List.of());
  }

}
