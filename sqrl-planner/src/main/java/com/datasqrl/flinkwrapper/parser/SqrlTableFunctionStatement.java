package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class SqrlTableFunctionStatement extends SqrlDefinition {

  private Map<Name, ParsedObject<String>> arguments;
  private List<Name> argumentIndexes;

  public SqrlTableFunctionStatement(ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody,
      SqrlComments comments, Map<Name, ParsedObject<String>> arguments, List<Name> argumentIndexes) {
    super(tableName, definitionBody, comments);
    this.arguments = arguments;
    this.argumentIndexes = argumentIndexes;
  }


}
