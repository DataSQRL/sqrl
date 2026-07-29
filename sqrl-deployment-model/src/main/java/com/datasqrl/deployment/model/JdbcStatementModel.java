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
package com.datasqrl.deployment.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Duration;
import java.util.List;

/** A rendered statement in a JDBC deployment file. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JdbcStatementModel(
    String name,
    Type type,
    String sql,
    String description,
    List<Field> fields,
    List<String> primaryKey,
    List<String> partitionKey,
    PartitionType partitionType,
    Integer numPartitions,
    Duration ttl) {

  public JdbcStatementModel(
      String name, Type type, String sql, String description, List<Field> fields) {
    this(name, type, sql, description, fields, null, null, null, null, null);
  }

  public enum Type {
    TABLE,
    VIEW,
    QUERY,
    INDEX,
    EXTENSION
  }

  public enum PartitionType {
    NONE,
    HASH,
    LIST,
    RANGE
  }

  public record Field(String name, String type, boolean nullable, String description) {}
}
