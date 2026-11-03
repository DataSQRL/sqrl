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
package com.datasqrl.engine.database.relational;

import com.datasqrl.deployment.model.JdbcStatementModel.Field;
import com.datasqrl.deployment.model.JdbcStatementModel.Type;
import com.datasqrl.planner.tags.TableTags;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;

@JsonDeserialize(as = GenericJdbcStatement.class)
public interface JdbcStatement {

  String getName();

  Type getType();

  String getSql();

  String getDescription();

  List<Field> getFields();

  @JsonIgnore
  default JdbcEngineCreateTable getEngineTable() {
    return null;
  }

  /** For views, the names of the tables the view reads - null when unknown */
  @JsonIgnore
  default List<String> getReferencedTables() {
    return null;
  }

  /**
   * For tables and views, the effective tags of the table or function mapped from tag name to value
   * - null when there are no tags
   */
  @JsonIgnore
  default Map<String, String> getTags() {
    return null;
  }

  /** The tags of the engine table's table analysis, null when there are none */
  static Map<String, String> getEngineTableTags(JdbcEngineCreateTable engineTable) {
    if (engineTable == null || engineTable.tableAnalysis() == null) {
      return null;
    }
    return toTagMap(engineTable.tableAnalysis().getTags());
  }

  /** Converts tags to the representation in deployment assets: null when empty */
  static Map<String, String> toTagMap(TableTags tags) {
    return tags == null || tags.isEmpty() ? null : tags.toValueMap();
  }
}
