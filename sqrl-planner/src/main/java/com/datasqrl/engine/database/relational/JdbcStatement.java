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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@Value
public class JdbcStatement {

  public enum Type {
    TABLE,
    VIEW,
    QUERY,
    INDEX,
    EXTENSION
  }

  /** The name of the table/view/query/index */
  String name;

  /** The type of statement */
  Type type;

  /** The SQL representation of this statement */
  String sql;

  /** The datatype for table, view, and query. Is null for other types. */
  @JsonIgnore RelDataType dataType;

  /** The datatype converted to a list of Field. It's null if there is no dataType. */
  List<Field> fields;

  public JdbcStatement(String name, Type type, String sql) {
    this(name, type, sql, null, null);
  }

  @JsonCreator
  public JdbcStatement(
      @JsonProperty("name") String name,
      @JsonProperty("type") Type type,
      @JsonProperty("sql") String sql,
      @JsonProperty("fields") List<Field> fields) {
    this(name, type, sql, null, fields);
  }

  public JdbcStatement(
      String name, Type type, String sql, RelDataType dataType, List<Field> fields) {
    this.name = name;
    this.type = type;
    this.sql = sql;
    this.dataType = dataType;
    this.fields = fields;
  }

  public record Field(String name, String type, boolean nullable) {}
}
