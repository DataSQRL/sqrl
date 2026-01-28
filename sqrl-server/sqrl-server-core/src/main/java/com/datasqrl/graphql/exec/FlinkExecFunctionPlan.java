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

import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Singular;

@Builder
public record FlinkExecFunctionPlan(@Singular List<FlinkExecFunction> functions)
    implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  public Optional<FlinkExecFunction> getFunction(String functionId) {
    return functions.stream().filter(f -> f.getFunctionId().equals(functionId)).findFirst();
  }

  public static FlinkExecFunctionPlan deserialize(Path path) {
    try (var in = new ObjectInputStream(Files.newInputStream(path))) {
      var obj = in.readObject();

      return (FlinkExecFunctionPlan) obj;
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize plan from: " + path, e);
    }
  }
}
