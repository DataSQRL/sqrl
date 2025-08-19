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
import java.util.List;
import lombok.Getter;

/**
 * A table function that passes the SQL directly through to the database engine for execution
 * (without parsing/validating). Requires a return type so we know what types of records it returns
 * (since we don't analyze it).
 */
@Getter
public class SqrlPassthroughTableFunctionStatement extends SqrlTableFunctionStatement {

  private final ParsedObject<String> returnType;

  public SqrlPassthroughTableFunctionStatement(
      ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody,
      AccessModifier access,
      SqrlComments comments,
      ParsedObject<String> signature,
      List<ParsedArgument> argumentsByIndex,
      ParsedObject<String> returnType) {
    super(tableName, definitionBody, access, comments, signature, argumentsByIndex);
    this.returnType = returnType;
  }
}
