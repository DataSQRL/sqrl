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

import java.util.List;

/**
 * The data contracts of a compilation, which are written to the deployment.
 *
 * @param documents the documents, each of which describes at least one table
 */
public record DataContracts(List<Document> documents) {

  public static final DataContracts EMPTY = new DataContracts(List.of());

  /**
   * A data contract that is written to the deployment.
   *
   * @param fileName the name of the file of the contract
   * @param content the content of the file, in the format of the contract
   */
  public record Document(String fileName, String content) {}
}
