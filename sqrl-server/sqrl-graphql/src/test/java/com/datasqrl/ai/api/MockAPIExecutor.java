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
import java.util.function.Function;
import lombok.NonNull;
import lombok.Value;

@Value
public class MockAPIExecutor implements APIQueryExecutor {

  ObjectMapper mapper = new ObjectMapper();
  Function<String, String> queryToResult;

  public static MockAPIExecutor of(@NonNull String uniformResult) {
    return new MockAPIExecutor(s -> uniformResult);
  }

  @Override
  public ObjectMapper getObjectMapper() {
    return mapper;
  }

  @Override
  public ValidationResult validate(@NonNull FunctionDefinition functionDef, JsonNode arguments) {
    return ValidationResult.VALID;
  }

  @Override
  public ValidationResult validate(APIQuery query) {
    return ValidationResult.VALID;
  }

  @Override
  public String executeQuery(APIQuery query, JsonNode arguments) throws IOException {
    return queryToResult.apply(query.query());
  }

  @Override
  public CompletableFuture<String> executeQueryAsync(APIQuery query, JsonNode arguments) {
    return CompletableFuture.completedFuture("mock write");
  }
}
