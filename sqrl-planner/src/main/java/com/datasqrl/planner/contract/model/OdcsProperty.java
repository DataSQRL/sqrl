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
package com.datasqrl.planner.contract.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import java.util.Map;
import lombok.Builder;

/**
 * A property of a schema object, which describes a column, a nested field, the items of an array,
 * or the key or value of a map. Only a column and a nested field have a name. It has the fields
 * that the compiler generates and compares; a contract file can have more.
 *
 * @param required {@code true} if the value cannot be null, otherwise not set, since {@code false}
 *     is the default
 * @param primaryKey {@code true} for a primary key column, otherwise not set
 * @param partitioned {@code true} for a partition column, otherwise not set
 * @param tags never null
 * @param properties never null
 */
@Builder(toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({
  "name",
  "logicalType",
  "logicalTypeOptions",
  "physicalType",
  "required",
  "primaryKey",
  "primaryKeyPosition",
  "partitioned",
  "partitionKeyPosition",
  "classification",
  "description",
  "tags",
  "properties",
  "items",
  "map"
})
public record OdcsProperty(
    String name,
    String logicalType,
    Map<String, Object> logicalTypeOptions,
    String physicalType,
    Boolean required,
    Boolean primaryKey,
    Integer primaryKeyPosition,
    Boolean partitioned,
    Integer partitionKeyPosition,
    String classification,
    String description,
    List<String> tags,
    List<OdcsProperty> properties,
    OdcsProperty items,
    OdcsMap map) {

  public OdcsProperty {
    tags = tags == null ? List.of() : tags;
    properties = properties == null ? List.of() : properties;
  }

  /** Whether the value cannot be null: a property without {@code required} can be null. */
  public boolean mustBeSet() {
    return Boolean.TRUE.equals(required);
  }
}
