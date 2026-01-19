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
package com.datasqrl.loaders;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * Loads functions from the classpath which applies to standard library functions that are part of
 * the DataSQRL distribution (in contrast to UDFs that the user provides via JARs - those are loaded
 * from a directory)
 */
@AllArgsConstructor
public class ClasspathFunctionLoader {

  public static final Set<Class<? extends FunctionDefinition>> FLINK_UDF_CLASSES =
      Set.of(
          UserDefinedFunction.class,
          ScalarFunction.class,
          AggregateFunction.class,
          TableFunction.class,
          TableAggregateFunction.class,
          ProcessTableFunction.class,
          AsyncScalarFunction.class,
          AsyncTableFunction.class,
          AsyncLookupFunction.class);

  private static final List<String> TRUNCATED_PACKAGE_PREFIX =
      List.of("com.datasqrl.flinkrunner.", "com.datasqrl.");

  private final HashMultimap<NamePath, FunctionDefinition> functionsByPackage;

  private static NamePath getNamePathFromFunction(FunctionDefinition function) {
    var packageName = function.getClass().getPackageName();
    // Remove certain pre-defined prefixes for more concise names of system functions
    for (var prefix : TRUNCATED_PACKAGE_PREFIX) {
      if (packageName.startsWith(prefix)) {
        packageName = packageName.substring(prefix.length());
      }
    }
    return NamePath.parse(packageName);
  }

  public ClasspathFunctionLoader() {
    functionsByPackage =
        FLINK_UDF_CLASSES.stream()
            .flatMap(this::loadClasses)
            .collect(
                HashMultimap::create,
                (multimap, function) -> multimap.put(getNamePathFromFunction(function), function),
                Multimap::putAll);
  }

  private Stream<? extends FunctionDefinition> loadClasses(
      Class<? extends FunctionDefinition> serviceInterface) {
    return StreamSupport.stream(ServiceLoader.load(serviceInterface).spliterator(), false);
  }

  public List<NamespaceObject> load(NamePath namePath) {
    if (!functionsByPackage.containsKey(namePath)) {
      return List.of();
    }

    return functionsByPackage.get(namePath).stream()
        .map(NamespaceObjectUtil::createNsObject)
        .collect(Collectors.toList());
  }
}
