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
package com.datasqrl.graphql.exec;

import com.datasqrl.graphql.server.FunctionExecutor;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Vertx;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;

@RequiredArgsConstructor
public class FlinkFunctionExecutor implements FunctionExecutor {

  private final Vertx vertx;
  private final Optional<FlinkExecFunctionPlan> optPlan;

  @Override
  public CompletableFuture<Object> execute(DataFetchingEnvironment env, String functionId) {
    var plan = optPlan.orElseThrow(() -> new IllegalStateException("Exec function plan not found"));
    var fn =
        plan.getFunction(functionId)
            .orElseThrow(
                () -> new IllegalArgumentException("Function " + functionId + " not found"));

    var inputType = fn.getInputType();
    validateInputFields(env, inputType);

    // Execute blocking function operations on Vert.x worker pool
    return vertx
        .executeBlocking(
            () -> {
              fn.instantiateFunction(Thread.currentThread().getContextClassLoader());

              var mapper = new RowDataMapper(inputType);
              var rowData = mapper.toRowData(env.getArguments());

              var internalRes = fn.execute(rowData);
              var res = mapper.fromRowData((GenericRowData) internalRes);

              return fn.isListOutput() ? res : res.get(0);
            },
            false) // false -> unordered execution (better concurrency)
        .toCompletionStage()
        .toCompletableFuture();
  }

  private void validateInputFields(DataFetchingEnvironment env, RowType inputType) {
    var missingFields =
        inputType.getFieldNames().stream()
            .filter(fieldName -> !env.getArguments().containsKey(fieldName))
            .collect(Collectors.toList());

    if (!missingFields.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot execute function. Missing required input fields: "
              + String.join(", ", missingFields));
    }
  }
}
