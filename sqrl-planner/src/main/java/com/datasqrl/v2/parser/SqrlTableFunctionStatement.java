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

  public boolean isRelationship() {
    return getPath().size()==2;
  }

  public Map<Integer, Integer> getArgIndexMap() {
    return IntStream.range(0, argumentsByIndex.size()).boxed().collect(Collectors.toMap(
        i -> i, i -> argumentsByIndex.get(i).index));
  }

  @Value
  @AllArgsConstructor
  public static class ParsedArgument implements ParsedField {
    ParsedObject<String> name;
    ParsedObject<String> type;
    RelDataType resolvedRelDataType;
    boolean isParentField;
    int index;

    public ParsedArgument(ParsedObject<String> name, ParsedObject<String> type, int index) {
      this(name, type, null, false, index);
    }

    public ParsedArgument(ParsedObject<String> name, int index) {
      this(name, null, null, true, index);
    }

    public ParsedArgument withName(ParsedObject<String> name) {
      return new ParsedArgument(name, type, resolvedRelDataType, isParentField, index);
    }

    public ParsedArgument withResolvedType(RelDataType resolvedRelDataType) {
      Preconditions.checkArgument(!hasResolvedType());
      return new ParsedArgument(name, type, resolvedRelDataType, isParentField, index);
    }

    public boolean hasResolvedType() {
      return resolvedRelDataType!=null;
    }
  }


}
