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
package com.datasqrl.util;

import com.datasqrl.function.FunctionMetadata;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

public class FunctionUtil {

  public static <T> Optional<T> getFunctionByNameFromClass(
      Class clazz, Class<T> assignableFrom, String name) {
    for (Field field : clazz.getDeclaredFields()) {
      try {
        if (Modifier.isStatic(field.getModifiers())
            && assignableFrom.isAssignableFrom(field.getType())) {
          if (field.getName().equalsIgnoreCase(name)) {
            return Optional.of((T) field.get(null));
          }
        }
      } catch (IllegalAccessException e) {

      }
    }
    return Optional.empty();
  }

  public static Optional<FunctionDefinition> getBridgedFunction(SqlOperator operator) {
    if (operator instanceof BridgingSqlFunction) {
      return Optional.of(
          ((org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction) operator)
              .getDefinition());
    } else if (operator instanceof BridgingSqlAggFunction) {
      return Optional.of(
          ((org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction) operator)
              .getDefinition());
    }
    return Optional.empty();
  }

  public static <C> Optional<C> getFunctionMetaData(
      FunctionDefinition functionDefinition, Class<C> functionClass) {
    return ServiceLoaderDiscovery.getAll(FunctionMetadata.class).stream()
        .filter(f -> f.getMetadataClass().equals(functionDefinition.getClass()))
        .filter(f -> functionClass.isAssignableFrom(f.getClass()))
        .map(f -> (C) f)
        .findFirst();
  }
}
