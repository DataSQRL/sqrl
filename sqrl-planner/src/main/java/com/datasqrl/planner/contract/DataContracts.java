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

import java.nio.file.Path;
import java.util.List;

/**
 * The data contracts of a compilation: the documents that are written to the deployment, and how
 * the contract files of the project were used.
 *
 * @param documents the documents, each of which describes at least one table
 * @param files the contract files of the project, sorted by path
 */
public record DataContracts(List<Document> documents, List<ContractFileUsage> files) {

  public static final DataContracts EMPTY = new DataContracts(List.of(), List.of());

  /**
   * A data contract that is written to the deployment.
   *
   * @param fileName the name of the file of the contract
   * @param content the content of the file, in the format of the contract
   */
  public record Document(String fileName, String content) {}

  /**
   * How a contract file of the project was used.
   *
   * @param file the contract file in the build folder
   * @param used false if the script next to the file is not part of the compilation
   * @param appliedTables the tables that entries of the file were applied to
   * @param unappliedEntries the entries of the file that match no table
   */
  public record ContractFileUsage(
      Path file, boolean used, List<String> appliedTables, List<String> unappliedEntries) {

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
      return unappliedEntries.isEmpty()
          ? line
          : line + " (not applied: " + String.join(", ", unappliedEntries) + ")";
    }
  }
}
