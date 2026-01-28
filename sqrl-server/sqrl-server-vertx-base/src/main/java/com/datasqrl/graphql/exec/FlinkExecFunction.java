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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.RowType;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class FlinkExecFunction implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  @Getter private final String functionId;
  @Getter private final String functionDescription;
  @Getter private final RowType inputType;
  @Getter private final RowType outputType;
  @Getter private final boolean listOutput;
  @JsonIgnore private final GeneratedFunction<FlatMapFunction<RowData, RowData>> function;

  @JsonIgnore private transient FlatMapFunction<RowData, RowData> instantiatedFunction;

  public void instantiateFunction(ClassLoader classLoader) {
    instantiatedFunction = function.newInstance(classLoader);
  }

  public String getCode() {
    return function.getCode();
  }

  public <T extends RowData> RowData execute(T input) {
    var res = execute(List.of(input));

    return res.get(0);
  }

  @SneakyThrows
  public List<RowData> execute(List<? extends RowData> inputs) {
    if (instantiatedFunction == null) {
      throw new IllegalStateException("Function %s not instantiated".formatted(functionId));
    }

    // Init exec function
    if (instantiatedFunction instanceof RichFlatMapFunction<RowData, RowData> richFn) {
      richFn.setRuntimeContext(createRuntimeContext());
      richFn.open(DefaultOpenContext.INSTANCE);
    }

    var result = new ArrayList<RowData>();
    var collector = new ListCollector<>(result);

    inputs.forEach(
        in -> {
          try {
            instantiatedFunction.flatMap(in, collector);
          } catch (Exception e) {
            throw new RuntimeException(
                "Could not execute function [%s] for [%s]"
                    .formatted(functionId, functionDescription),
                e);
          }
        });

    return result;
  }

  /**
   * Creates a dummy runtime context for Flink functions that require initialization.
   *
   * <p>This ensures that if the execution function has {@code open()} logic, it will be executed
   * properly.
   *
   * @return a dummy {@link RuntimeContext} with minimal configuration
   */
  private RuntimeContext createRuntimeContext() {
    var taskInfo = new TaskInfoImpl(functionDescription, 1, 0, 1, 0);

    return new RuntimeUDFContext(
        taskInfo,
        Thread.currentThread().getContextClassLoader(),
        null,
        Map.of(),
        Map.of(),
        UnregisteredMetricsGroup.createOperatorMetricGroup());
  }
}
