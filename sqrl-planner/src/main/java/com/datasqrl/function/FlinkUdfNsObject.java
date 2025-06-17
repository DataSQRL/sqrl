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
package com.datasqrl.function;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.module.FunctionNamespaceObject;
import java.net.URL;
import java.util.Optional;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;

@Value
@Slf4j
public class FlinkUdfNsObject implements FunctionNamespaceObject<FunctionDefinition> {
  Name name;
  FunctionDefinition function;
  private final String sqlName;
  Optional<URL> jarUrl;

  public FlinkUdfNsObject(
      String name, FunctionDefinition function, String sqlName, Optional<URL> jarUrl) {
    this.name = Name.system(name);
    this.function = function;
    this.sqlName = sqlName;
    this.jarUrl = jarUrl;
  }

  public static String getFunctionName(FunctionDefinition function) {
    if (function instanceof BuiltInFunctionDefinition) {
      return ((BuiltInFunctionDefinition) function).getName();
    }
    return getFunctionNameFromClass(function.getClass()).getDisplay();
  }

  public static Name getFunctionNameFromClass(Class clazz) {
    var fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return Name.system(fctName);
  }
}
