package com.datasqrl.v2.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Represents a table function definition with arguments
 * A table function that's nested within another table is a relationship
 */

@Getter
public class SqrlTableFunctionStatement extends SqrlDefinition {

  private final ParsedObject<String> signature;
  private final List<ParsedArgument> argumentsByIndex;

  public SqrlTableFunctionStatement(ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody, AccessModifier access,
      SqrlComments comments, ParsedObject<String> signature, List<ParsedArgument> argumentsByIndex) {
    super(tableName, definitionBody, access, comments);
    this.signature = signature;
    this.argumentsByIndex = argumentsByIndex;
  }

  public boolean isRelationship() {
    return getPath().size()==2;
  }


  @Value
  @AllArgsConstructor
  public static class ParsedArgument {
    ParsedObject<String> name;
    RelDataType resolvedRelDataType;
    boolean isParentField;
    int index;

    public ParsedArgument(ParsedObject<String> name, boolean isParentField, int index) {
      this(name,  null, isParentField, index);
    }

    public ParsedArgument withResolvedType(RelDataType resolvedRelDataType, int index) {
      Preconditions.checkArgument(!hasResolvedType());
      return new ParsedArgument(name, resolvedRelDataType, isParentField, index);
    }

    public boolean hasResolvedType() {
      return resolvedRelDataType!=null;
    }
  }


}
