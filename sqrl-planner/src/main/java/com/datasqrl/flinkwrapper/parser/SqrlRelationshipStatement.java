package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SqrlRelationshipStatement extends SqrlTableFunctionStatement {

  ParsedObject<NamePath> relationshipPath;

  public SqrlRelationshipStatement(ParsedObject<NamePath> relationshipPath,
      ParsedObject<String> definitionBody, SqrlComments comments,
      Map<Name, ParsedObject<String>> arguments, List<Name> argumentIndexes) {
    super(relationshipPath.map(x -> NamePath.of(x.getLast())),
        definitionBody.map(body -> String.format("SELECT * FROM %s this %s", relationshipPath.get().popLast(), body)),
            AccessModifier.INHERIT, comments, arguments, argumentIndexes);
    this.relationshipPath = relationshipPath;
  }

  public NamePath getPath() {
    return relationshipPath.get();
  }

}
