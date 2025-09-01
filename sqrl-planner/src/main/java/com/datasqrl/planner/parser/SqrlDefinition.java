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
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A partially parsed SQRL definition. Some elements are extracted but the body of the definition is
 * kept as a string to be passed to the Flink parser for parsing and conversion.
 *
 * <p>As such, we try to do our best to keep offsets to map errors back by preserving position.
 */
@AllArgsConstructor
@Getter
public abstract class SqrlDefinition implements SqrlStatement {

  final ParsedObject<NamePath> tableName;
  final ParsedObject<String> definitionBody;
  final AccessModifier access;
  final SqrlComments comments;

  /**
   * Generates the SQL string that gets passed to the Flink parser for this statement
   *
   * @param sqrlEnv
   * @param stack
   * @return
   */
  public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    var prefix = getPrefix();
    return prefix + definitionBody.get();
  }

  String getPrefix() {
    return "CREATE VIEW `%s` AS ".formatted(tableName.get().getLast().getDisplay());
  }

  public NamePath getPath() {
    return tableName.get();
  }

  public boolean isTable() {
    return tableName.get().size() == 1;
  }

  public boolean isRelationship() {
    return !isTable();
  }

  /**
   * Maps the FileLocation for errors
   *
   * @param location
   * @return
   */
  @Override
  public FileLocation mapSqlLocation(FileLocation location) {
    return definitionBody
        .getFileLocation()
        .add(SQLStatement.removeFirstRowOffset(location, getPrefix().length()));
  }
}
