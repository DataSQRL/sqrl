package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.Getter;
import lombok.Value;
import scala.tools.nsc.interpreter.Parsed;

@Getter
public class SqrlTableFunctionStatement extends SqrlDefinition {

  private final List<ParsedArgument> arguments;
  private final List<ParsedArgument> argumentsByIndex;

  public SqrlTableFunctionStatement(ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody, AccessModifier access,
      SqrlComments comments, List<ParsedArgument> arguments, List<ParsedArgument> argumentsByIndex) {
    super(tableName, definitionBody, access, comments);
    this.arguments = arguments;
    this.argumentsByIndex = argumentsByIndex;
  }

  @Value
  public static class ParsedArgument {
    ParsedObject<String> name;
    ParsedObject<String> type;
    boolean isParentField;
    int ordinal;
  }


}
