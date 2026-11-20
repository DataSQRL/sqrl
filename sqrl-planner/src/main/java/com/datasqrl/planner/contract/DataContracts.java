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

import com.datasqrl.planner.contract.model.OdcsContract;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import java.util.List;

/**
 * The data contracts of a compilation: the documents that are written to the deployment, and how
 * the contract files of the project were used.
 *
 * @param documents the documents, each with at least one schema object
 * @param files the contract files of the project, sorted by path
 */
public record DataContracts(List<Document> documents, List<ContractFileUsage> files) {

  public static final DataContracts EMPTY = new DataContracts(List.of(), List.of());

  /**
   * A data contract that is written to the deployment.
   *
   * @param name the name of the document, which is its id and its file name without the extension
   * @param schema the schema objects: the ones that contract files provide, with all their fields,
   *     and generated ones
   */
  public record Document(String name, List<JsonNode> schema) {

    public String fileName() {
      return OdcsConstants.fileName(name);
    }

    public OdcsContract toContract() {
      return new OdcsContract(
          OdcsVersion.LATEST.getApiVersion(),
          OdcsConstants.KIND,
          name,
          name,
          OdcsConstants.GENERATED_VERSION,
          schema);
    }
  }

  /**
   * How a contract file of the project was used.
   *
   * @param file the contract file in the build folder
   * @param used false if the script next to the file is not part of the compilation
   * @param appliedTables the tables that schema objects of the file were applied to
   * @param unappliedObjects the schema objects of the file that match no table
   */
  public record ContractFileUsage(
      Path file, boolean used, List<String> appliedTables, List<String> unappliedObjects) {

    /**
     * @param shownFile the path of the file as the user knows it
     * @return the line of the compile summary for the file
     */
    public String describe(Path shownFile) {
      if (!used) {
        return shownFile + ": not used in this compile";
      }
      var line =
          shownFile
              + ": "
              + (appliedTables.isEmpty() ? "no table" : String.join(", ", appliedTables));
      return unappliedObjects.isEmpty()
          ? line
          : line + " (not applied: " + String.join(", ", unappliedObjects) + ")";
    }
  }
}
