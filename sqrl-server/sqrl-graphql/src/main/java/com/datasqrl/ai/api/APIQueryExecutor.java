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
package com.datasqrl.ai.api;

import com.datasqrl.ai.tool.FunctionDefinition;
import com.datasqrl.ai.tool.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

/** Interface for querying and writing to an API */
public interface APIQueryExecutor {

  ObjectMapper getObjectMapper();

  /**
   * Validates that the provided arguments are valid for the given {@link FunctionDefinition}.
   *
   * @param functionDef
   * @param arguments
   * @return
   */
  ValidationResult validate(@NonNull FunctionDefinition functionDef, JsonNode arguments);

  /**
   * Validates whether the provided query is a valid query for this API executor.
   *
   * @param query the query to validate
   * @return the validation result
   */
  ValidationResult validate(APIQuery query);

  /**
   * Executes the given query with the provided arguments against the API and returns the result as
   * a String.
   *
   * @param query the query to execute
   * @param arguments the arguments for the query
   * @return The result of the query as a String
   * @throws IOException if the connection to the API failed or the query could not be executed
   */
  String executeQuery(APIQuery query, JsonNode arguments) throws IOException;

  /**
   * Executes an asynchronous request against the API for the given query with arguments.
   *
   * @param query the query to execute
   * @param arguments the arguments for the query
   * @return A future for the result
   */
  default CompletableFuture<String> executeQueryAsync(APIQuery query, JsonNode arguments)
      throws IOException {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return executeQuery(query, arguments);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
