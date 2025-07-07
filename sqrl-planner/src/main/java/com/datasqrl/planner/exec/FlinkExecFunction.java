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
package com.datasqrl.planner.exec;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

@AllArgsConstructor
public class FlinkExecFunction implements Serializable {
  private static final long serialVersionUID = 1L;

  @Getter private final String functionId;
  @Getter private final String functionDescription;
  @Getter private final RowType inputType;
  @Getter private final RowType outputType;
  @JsonIgnore private final GeneratedFunction<FlatMapFunction<RowData, RowData>> function;
  @JsonIgnore private transient FlatMapFunction<RowData, RowData> instantiatedFunction = null;

  public void instantiateFunction(ClassLoader classLoader) {
    FlatMapFunction<RowData, RowData> instantiatedFunction = function.newInstance(classLoader);
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

  public List<RowData> execute(
      FlatMapFunction<RowData, RowData> function, List<? extends RowData> inputs) {
    if (instantiatedFunction == null)
      throw new IllegalStateException("Function %s not instantiated".formatted(functionId));
    List<RowData> result = new java.util.ArrayList<>();
    org.apache.flink.util.Collector<org.apache.flink.table.data.RowData> collector =
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
            function.flatMap(in, collector);
          } catch (Exception e) {
            throw new RuntimeException(
                "Could not execute function [%s] for [%s]"
                    .formatted(functionId, functionDescription),
                e);
          }
        });
    return result;
  }

  @Builder
  public record Plan(@Singular List<FlinkExecFunction> functions) {

    public Optional<FlinkExecFunction> getFunction(String functionId) {
      return functions.stream().filter(f -> f.getFunctionId().equals(functionId)).findFirst();
    }

    public static Plan deserialize(Path path) {
      try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(path))) {
        Object obj = in.readObject();
        return (Plan) obj;
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException("Failed to deserialize Plan from: " + path, e);
      }
    }
  }
}
