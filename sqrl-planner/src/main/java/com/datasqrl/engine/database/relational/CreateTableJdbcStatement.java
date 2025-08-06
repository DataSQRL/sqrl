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

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.collections4.CollectionUtils;

/**
 * A CREATE TABLE {@link JdbcStatement} which tracks additional information about the physical table
 * and create the SQL string on demand so it can be optimized.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"name", "type", "sql", "description", "fields"})
@Value
@AllArgsConstructor
public class CreateTableJdbcStatement implements JdbcStatement {

  /** The name of the table/view/query/index */
  String name;

  /** The docstring for this table if defined */
  String description;

  /** Fields for the columns in this table */
  List<Field> fields;

  /** List of primary keys in order */
  List<String> primaryKey;

  /** List of partition keys */
  List<String> partitionKey;

  /** The partitioning type */
  PartitionType partitionType;

  /** The number of partitions the user wishes to create - defaults to parallelism */
  int numPartitions;

  /** The time-to-live of records in this table - ZERO to disable */
  Duration ttl;

  /** The engine table - nullable and not set when deserialized */
  @JsonIgnore JdbcEngineCreateTable engineTable;

  /** The DDL factory to create the SQL string - nullable and not set when deserialized */
  @JsonIgnore CreateTableDdlFactory ddlFactory;

  @JsonCreator
  public CreateTableJdbcStatement(
      @JsonProperty("name") String name,
      @JsonProperty("description") String description,
      @JsonProperty("fields") List<Field> fields,
      @JsonProperty("primaryKey") List<String> primaryKey,
      @JsonProperty("partitionKey") List<String> partitionKey,
      @JsonProperty("partitionType") PartitionType partitionType,
      @JsonProperty("numPartitions") int numPartitions,
      @JsonProperty("ttl") Duration ttl) {
    this.name = name;
    this.description = description;
    this.fields = fields;
    this.primaryKey = primaryKey;
    this.partitionKey = partitionKey;
    this.partitionType = partitionType;
    this.numPartitions = numPartitions;
    this.ttl = ttl;
    this.engineTable = null;
    this.ddlFactory = null;
  }

  @Override
  public Type getType() {
    return Type.TABLE;
  }

  @Override
  public String getSql() {
    Preconditions.checkNotNull(ddlFactory);
    return getSql(ddlFactory);
  }

  public CreateTableJdbcStatement updatePrimaryKey(List<String> newPk) {
    checkArgument(
        CollectionUtils.isEqualCollection(primaryKey, newPk),
        "Primary keys are not compatible: %s vs %s",
        primaryKey,
        newPk);

    return new CreateTableJdbcStatement(
        name,
        description,
        fields,
        newPk,
        partitionKey,
        partitionType,
        numPartitions,
        ttl,
        engineTable,
        ddlFactory);
  }

  public String getSql(CreateTableDdlFactory ddlFactory) {
    return ddlFactory.createTableDdl(this);
  }

  public enum PartitionType {
    NONE,
    HASH,
    LIST,
    RANGE
  }

  @FunctionalInterface
  public interface CreateTableDdlFactory {

    String createTableDdl(CreateTableJdbcStatement stmt);
  }
}
