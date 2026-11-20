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

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import java.util.Optional;

/**
 * The data contract of a table that a {@code CREATE TABLE} statement defines. It identifies the
 * definition: the same statement that is imported or exported more than once has equal table
 * contracts.
 *
 * @param definingFile the file that defines the table, which is only used to identify the
 *     definition and in messages
 * @param tableName the name of the table in its statement, not an import alias
 * @param attached the schema object that a contract file provides for the table, if any
 * @param schemaTableFile the file that defines the schema table that the table is or extends,
 *     directly or through other tables. Its contract applies to the table if the contract of the
 *     table's own file has no schema object for it
 */
public record TableContract(
    Optional<Path> definingFile,
    String tableName,
    Optional<AttachedContract> attached,
    Optional<Path> schemaTableFile) {

  /** The contract of a table whose defining file is unknown, such as a generated table. */
  public static TableContract none(String tableName) {
    return new TableContract(Optional.empty(), tableName, Optional.empty(), Optional.empty());
  }

  public TableContract withSchemaTableFile(Optional<Path> schemaTableFile) {
    return new TableContract(definingFile, tableName, attached, schemaTableFile);
  }

  /**
   * A schema object that a contract file provides for a table.
   *
   * @param file the contract file
   * @param pointer the JSON pointer of the schema object in the file
   * @param schemaObject the schema object, with all fields that the file has
   */
  public record AttachedContract(DataContractFile file, String pointer, JsonNode schemaObject) {}
}
