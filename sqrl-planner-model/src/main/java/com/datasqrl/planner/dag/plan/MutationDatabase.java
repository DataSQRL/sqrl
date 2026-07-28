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
package com.datasqrl.planner.dag.plan;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

/** The serialized model of the mutation database file (pipeline_mutation_database.json). */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record MutationDatabase(List<Table> tables) {

  public record Table(
      String canonicalName,
      String engine,
      String createTableSql,
      TableDefinition definition,
      Map<String, String> configOptions,
      String documentation) {}

  public record TableDefinition(
      List<ColumnDefinition> columns, List<String> primaryKey, List<String> partitionKey) {}

  public record ColumnDefinition(String name, String spec, String documentation) {}
}
