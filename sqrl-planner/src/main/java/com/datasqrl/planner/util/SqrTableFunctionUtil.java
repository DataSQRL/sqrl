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
package com.datasqrl.planner.util;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.planner.tables.SqrlTableFunction;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqrTableFunctionUtil {
  public static Optional<SqrlTableFunction> getTableFunctionFromPath(
      List<SqrlTableFunction> tableFunctions, NamePath path) {
    final List<SqrlTableFunction> tableFunctionsAtPath =
        tableFunctions.stream()
            .filter(tableFunction -> tableFunction.getFullPath().equals(path))
            .collect(Collectors.toList());
    assert (tableFunctionsAtPath.size() <= 1); // no overloading
    return tableFunctionsAtPath.isEmpty()
        ? Optional.empty()
        : Optional.of(tableFunctionsAtPath.get(0));
  }
}
