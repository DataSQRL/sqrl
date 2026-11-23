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
package com.datasqrl.planner.contract.odcs.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import lombok.Builder;

/**
 * A schema object of a data contract, which describes one table. It has the fields that the
 * compiler generates and compares; a contract file can have more.
 *
 * @param tags never null
 * @param properties never null
 */
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({
  "name",
  "physicalName",
  "physicalType",
  "logicalType",
  "description",
  "tags",
  "properties"
})
public record OdcsSchemaObject(
    String name,
    String physicalName,
    String physicalType,
    String logicalType,
    String description,
    List<String> tags,
    List<OdcsProperty> properties) {

  public static final String LOGICAL_TYPE = "object";

  public OdcsSchemaObject {
    tags = tags == null ? List.of() : tags;
    properties = properties == null ? List.of() : properties;
  }
}
