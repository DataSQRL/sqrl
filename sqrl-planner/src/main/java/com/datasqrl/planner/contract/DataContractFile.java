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
package com.datasqrl.planner.contract;

import com.datasqrl.error.ErrorCollector;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A data contract file that sits next to a script, and the tables of the compilation that its
 * schema objects were applied to.
 */
@RequiredArgsConstructor
public final class DataContractFile {

  /**
   * @param name the name of the schema object as the file writes it
   * @param pointer the JSON pointer of the schema object in the file
   */
  public record SchemaObject(String name, String pointer, JsonNode node) {}

  /** A table that a schema object was applied to. */
  public record AppliedTable(Path definingFile, String tableName) {}

  @Getter private final Path path;
  private final String content;
  private final JsonNode root;
  @Getter private final OdcsVersion version;
  private final YamlLocations locations;

  /** The schema objects by their name in lower case, in the order of the file */
  private final Map<String, SchemaObject> schemaObjects;

  /** The table that each schema object was applied to, by the name of the object in lower case */
  private final Map<String, AppliedTable> appliedTables = new LinkedHashMap<>();

  public Optional<SchemaObject> getSchemaObject(String tableName) {
    return Optional.ofNullable(schemaObjects.get(key(tableName)));
  }

  /**
   * Records that the schema object describes the table, unless it already describes another one.
   *
   * @return the table that the schema object describes
   */
  AppliedTable markApplied(SchemaObject schemaObject, AppliedTable table) {
    var applied = appliedTables.putIfAbsent(key(schemaObject.name()), table);
    return applied == null ? table : applied;
  }

  /**
   * The names of the tables that schema objects of this file were applied to, in the order the
   * tables were planned.
   */
  public List<String> getAppliedTableNames() {
    return appliedTables.values().stream().map(AppliedTable::tableName).toList();
  }

  /** The names of the schema objects that were not applied to a table, in file order. */
  public List<String> getUnappliedObjectNames() {
    return schemaObjects.entrySet().stream()
        .filter(entry -> !appliedTables.containsKey(entry.getKey()))
        .map(entry -> entry.getValue().name())
        .toList();
  }

  /** Whether a schema object declares tags or a classification, on itself or on a property. */
  public boolean declaresTags() {
    for (var schemaObject : root.path(OdcsConstants.SCHEMA_FIELD)) {
      if (!schemaObject.path(OdcsConstants.TAGS_FIELD).isEmpty()) {
        return true;
      }
      for (var property : schemaObject.path(OdcsConstants.PROPERTIES_FIELD)) {
        if (!property.path(OdcsConstants.TAGS_FIELD).isEmpty()
            || property.hasNonNull(OdcsConstants.CLASSIFICATION_FIELD)) {
          return true;
        }
      }
    }
    return false;
  }

  /** The error collector that reports at the value with the given JSON pointer in this file. */
  public ErrorCollector errorsAt(ErrorCollector errors, String pointer) {
    return errors.withScript(path, content).atFile(locations.locate(pointer));
  }

  static String key(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
