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

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorLocation.FileLocation;
import lombok.Value;

/**
 * Represents a {@code CREATE NAMESPACE <name> ( <params> );} statement that groups queries and
 * mutations under a GraphQL sub-object (e.g. {@code backend { ... }}) and declares parameters
 * shared by every function in the namespace. A parameter with {@code METADATA FROM 'auth....'} is a
 * hidden JWT claim; a parameter without it becomes an argument exposed on the namespace field
 * itself. Functions join the namespace via the {@code <name>.func} path prefix and reference the
 * shared parameters as {@code :<name>.<param>}.
 */
@Value
public class SqrlCreateNamespaceStatement implements SqrlStatement {

  ParsedObject<Name> name;
  ParsedObject<String> params;
  SqrlComments comments;

  @Override
  public FileLocation getDefaultLocation() {
    return name.getFileLocation();
  }
}
