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
package com.datasqrl;

import com.datasqrl.function.FlinkUdfNsObject;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.flink.table.functions.FunctionDefinition;

public class NamespaceObjectUtil {

  public static FlinkUdfNsObject createNsObject(FunctionDefinition function) {
    Preconditions.checkArgument(
        function instanceof FunctionDefinition,
        "All SQRL function implementations must extend FunctionDefinition: %s",
        function.getClass());
    var fnName = function.getClass().getSimpleName();

    return new FlinkUdfNsObject(fnName, function, fnName, Optional.empty());
  }
}
