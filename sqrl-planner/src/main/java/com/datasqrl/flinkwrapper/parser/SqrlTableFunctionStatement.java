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
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

@Getter
public class SqrlTableFunctionStatement extends SqrlDefinition {

  private final Map<Name, ParsedObject<String>> arguments;
  private final List<Name> argumentIndexes;

  public SqrlTableFunctionStatement(ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody, boolean isSubscription,
      SqrlComments comments, Map<Name, ParsedObject<String>> arguments, List<Name> argumentIndexes) {
    super(tableName, definitionBody, isSubscription, comments);
    this.arguments = arguments;
    this.argumentIndexes = argumentIndexes;
  }


}
