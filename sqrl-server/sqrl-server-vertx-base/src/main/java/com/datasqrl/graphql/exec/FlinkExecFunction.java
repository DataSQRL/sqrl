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
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class FlinkExecFunction implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  @Getter private final String functionId;
  @Getter private final String functionDescription;
  @Getter private final RowType inputType;
  @JsonIgnore private final GeneratedFunction<FlatMapFunction<RowData, RowData>> function;

  @JsonIgnore private transient FlatMapFunction<RowData, RowData> instantiatedFunction;

  public void instantiateFunction(ClassLoader classLoader) {
    instantiatedFunction = function.newInstance(classLoader);
    // TODO: Do we need to instantiate the runtime context?
    /*
    if (fn instanceof org.apache.flink.api.common.functions.RichFunction rich) {
      try {
        RuntimeContext rtCtx = null; //mirror MockStreamingRuntimeContext from Apache Flink
        rich.setRuntimeContext(rtCtx);
        rich.open(new OpenContext() {
        });
      } catch (Exception e) {
        throw new RuntimeException("Could not instantiate function [%s] for [%s]"
            .formatted(functionId, functionDescription), e);
      }
    }
     */
  }

  public String getCode() {
    return function.getCode();
  }

  public List<RowData> execute(List<? extends RowData> inputs) {
    if (instantiatedFunction == null) {
      throw new IllegalStateException("Function %s not instantiated".formatted(functionId));
    }

    var result = new ArrayList<RowData>();
    var collector =
        new Collector<RowData>() {
          @Override
          public void collect(RowData rowData) {
            result.add(rowData);
          }

          @Override
          public void close() {}
        };

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
}
