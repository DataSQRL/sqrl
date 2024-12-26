package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.sql.SqlIdentifier;

public class SqrlRelationshipStatement extends SqrlDefinition {

  public static final String RELATIONSHP_TABLE_NAME = "__Relationship";
  private static AtomicInteger counter = new AtomicInteger(0);

  ParsedObject<NamePath> rootTable;
  ParsedObject<Name> relationshipName;

  public SqrlRelationshipStatement(ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody,
      SqrlComments comments, ParsedObject<Name> relationshipName) {
    super(tableName.map(x -> NamePath.of(RELATIONSHP_TABLE_NAME+counter.incrementAndGet())),
        definitionBody.map(body -> String.format("SELECT * FROM %s this %s", tableName.get(), body)),
            comments);
    this.relationshipName = relationshipName;
    this.rootTable = tableName;
  }

}
