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
import java.util.Map;

/**
 * A rendered statement in a JDBC deployment file.
 *
 * @param logicalName for tables, the SQRL table name substituted for {@code ${sqrl:table-name}} in
 *     the connector's table name template; not unique, a mutation table and its materialization
 *     share it, so key on {@code (logicalName, role)}
 * @param role for tables, whether the table stores a mutation or a table the pipeline materializes
 * @param referencedTables for views, the sorted names of the {@code TABLE} statements the view
 *     reads; not set when the compiler cannot determine them, such as for pass-through views
 * @param tags for tables and views, the effective tags of the SQRL table or function, mapped from
 *     tag name to value ({@code true} for boolean tags); not set when there are no tags
 */
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
    Duration ttl,
    String logicalName,
    Role role,
    List<String> referencedTables,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, String> tags) {

  public JdbcStatementModel(
      String name, Type type, String sql, String description, List<Field> fields) {
    this(name, type, sql, description, fields, null, null, null, null, null);
  }

  public JdbcStatementModel(
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
    this(
        name,
        type,
        sql,
        description,
        fields,
        primaryKey,
        partitionKey,
        partitionType,
        numPartitions,
        ttl,
        null,
        null);
  }

  public JdbcStatementModel(
      String name,
      Type type,
      String sql,
      String description,
      List<Field> fields,
      List<String> primaryKey,
      List<String> partitionKey,
      PartitionType partitionType,
      Integer numPartitions,
      Duration ttl,
      String logicalName,
      Role role) {
    this(
        name,
        type,
        sql,
        description,
        fields,
        primaryKey,
        partitionKey,
        partitionType,
        numPartitions,
        ttl,
        logicalName,
        role,
        null);
  }

  public JdbcStatementModel(
      String name,
      Type type,
      String sql,
      String description,
      List<Field> fields,
      List<String> primaryKey,
      List<String> partitionKey,
      PartitionType partitionType,
      Integer numPartitions,
      Duration ttl,
      String logicalName,
      Role role,
      List<String> referencedTables) {
    this(
        name,
        type,
        sql,
        description,
        fields,
        primaryKey,
        partitionKey,
        partitionType,
        numPartitions,
        ttl,
        logicalName,
        role,
        referencedTables,
        null);
  }

  public enum Type {
    TABLE,
    VIEW,
    QUERY,
    INDEX,
    EXTENSION
  }

  public enum Role {
    MUTATION,
    MATERIALIZATION
  }

  public enum PartitionType {
    NONE,
    HASH,
    LIST,
    RANGE
  }

  public record Field(String name, String type, boolean nullable, String description) {}
}
