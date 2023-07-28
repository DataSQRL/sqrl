/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//Copied from flink as we incrementally phase out flink code for sqrl code
package com.datasqrl.functions.flink;

import com.datasqrl.schema.TypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;

public class FunctionConverter {

  static EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
  static CatalogManager catalogManager = CatalogManager.newBuilder()
      .classLoader(FunctionConverter.class.getClassLoader())
      .config(TableConfig.getDefault())
      .defaultCatalog(
          settings.getBuiltInCatalogName(),
          new GenericInMemoryCatalog(
              settings.getBuiltInCatalogName(),
              settings.getBuiltInDatabaseName()))
      .executionConfig(new ExecutionConfig())
      .build();
  public static SqlFunction convert(String name, FunctionDefinition definition) {
    final TypeInference typeInference;

    DataTypeFactory dataTypeFactory = catalogManager.getDataTypeFactory();
    try {
      typeInference = definition.getTypeInference(dataTypeFactory);
    } catch (Throwable t) {
      throw new ValidationException(
          String.format(
              "An error occurred in the type inference logic of function '%s'.",
              name),
          t);
    }

    final SqlFunction function;
    if (definition.getKind() == FunctionKind.AGGREGATE
        || definition.getKind() == FunctionKind.TABLE_AGGREGATE) {
      throw new RuntimeException("Agg functions not yet supported");
//      function =
//         new BridgingSqlAggFunction(
//              dataTypeFactory,
//              typeFactory,
//              SqlKind.OTHER_FUNCTION,
//              resolvedFunction,
//              typeInference);
    } else {
      function =
          new BridgingSqlFunction(
              name,
              dataTypeFactory,
              (FlinkTypeFactory)TypeFactory.getTypeFactory(),
              null,
              SqlKind.OTHER_FUNCTION,
              definition,
              typeInference);
    }

    return function;
  }
}
