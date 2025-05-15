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

/**
 * A partially parsed SQRL definition. Some elements are extracted but the body of the definition is
 * kept as a string to be passed to the Flink parser for parsing and conversion.
 *
 * <p>As such, we try to do our best to keep offsets so we can map errors back.
 */
public class SqrlTableDefinition extends SqrlDefinition implements StackableStatement {

  public SqrlTableDefinition(
      ParsedObject<NamePath> tableName,
      ParsedObject<String> definitionBody,
      AccessModifier access,
      SqrlComments comments) {
    super(tableName, definitionBody, access, comments);
  }

  @Override
  public boolean isRoot() {
    return true;
  }
}
