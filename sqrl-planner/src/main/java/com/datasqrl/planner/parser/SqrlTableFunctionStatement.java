/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Represents a table function definition with arguments A table function that's nested within
 * another table is a relationship
 */
@Getter
public class SqrlTableFunctionStatement extends SqrlDefinition {

  private final ParsedObject<String> signature;
  private final List<ParsedArgument> argumentsByIndex;

  public SqrlTableFunctionStatement(
      ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody,
      AccessModifier access,
      SqrlComments comments,
      ParsedObject<String> signature,
      List<ParsedArgument> argumentsByIndex) {
    super(tableName, definitionBody, access, comments);
    this.signature = signature;
    this.argumentsByIndex = argumentsByIndex;
  }

  @Override
  public boolean isRelationship() {
    return getPath().size() == 2;
  }

  @Value
  @AllArgsConstructor
  public static class ParsedArgument {
    ParsedObject<String> name;
    RelDataType resolvedRelDataType;
    boolean isParentField;
    int index;

    public ParsedArgument(ParsedObject<String> name, boolean isParentField, int index) {
      this(name, null, isParentField, index);
    }

    public ParsedArgument withResolvedType(RelDataType resolvedRelDataType, int index) {
      Preconditions.checkArgument(!hasResolvedType());
      return new ParsedArgument(name, resolvedRelDataType, isParentField, index);
    }

    public boolean hasResolvedType() {
      return resolvedRelDataType != null;
    }
  }
}
