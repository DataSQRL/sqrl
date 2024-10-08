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
package com.datasqrl.flink;

import com.datasqrl.calcite.type.TypeFactory;
import java.util.Optional;
import javax.swing.text.html.Option;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogStoreHolder;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.types.inference.TypeInference;

@AllArgsConstructor
@Slf4j
public class FlinkConverter {

  static EnvironmentSettings settings = EnvironmentSettings.newInstance().build();

  static final CatalogStoreFactory catalogStoreFactory = TableFactoryUtil.findAndCreateCatalogStoreFactory(
      settings.getConfiguration(), settings.getUserClassLoader());
  public static CatalogManager catalogManager = CatalogManager.newBuilder()
      .classLoader(FlinkConverter.class.getClassLoader())
      .config(TableConfig.getDefault())
      .defaultCatalog(
          settings.getBuiltInCatalogName(),
          new GenericInMemoryCatalog(
              settings.getBuiltInCatalogName(),
              settings.getBuiltInDatabaseName()))
      .executionConfig(new ExecutionConfig())
      .catalogStoreHolder(CatalogStoreHolder.newBuilder()
          .catalogStore(catalogStoreFactory.createCatalogStore())
          .factory(TableFactoryUtil.findAndCreateCatalogStoreFactory(
                  settings.getConfiguration(), settings.getUserClassLoader()))
          .config(TableConfig.getDefault())
          .classloader(settings.getUserClassLoader())
          .build())
      .build();

  TypeFactory typeFactory;

  public static FlinkTypeFactory flinkTypeFactory = new FlinkTypeFactory(FlinkConverter.class.getClassLoader(),
      FlinkTypeSystem.INSTANCE);

  public Optional<SqlFunction> convertFunction(String flinkName, FunctionDefinition definition) {
    final TypeInference typeInference;

    DataTypeFactory dataTypeFactory = catalogManager.getDataTypeFactory();
    try {
      typeInference = definition.getTypeInference(dataTypeFactory);
    } catch (Throwable t) {
      throw new ValidationException(
          String.format(
              "An error occurred in the type inference logic of function '%s'.",
              flinkName),
          t);
    }

    if(definition.getKind() == FunctionKind.SCALAR || definition.getKind() == FunctionKind.TABLE) {
      return Optional.of(
          BridgingSqlFunction.of(dataTypeFactory,
              flinkTypeFactory,
              new RexFactory(flinkTypeFactory, ()->null, ()->null, (x)->null),
              SqlKind.OTHER_FUNCTION,
              ContextResolvedFunction.permanent(
                  FunctionIdentifier.of(flinkName), definition),
              typeInference));
    } else if (definition.getKind() == FunctionKind.AGGREGATE
        || definition.getKind() == FunctionKind.TABLE_AGGREGATE){
      return Optional.of(BridgingSqlAggFunction.of(dataTypeFactory,
              flinkTypeFactory,
              SqlKind.OTHER_FUNCTION,
              ContextResolvedFunction.permanent(
                  FunctionIdentifier.of(flinkName), definition),
              typeInference));
    } else {
      log.info("Could not register: " + flinkName);
      return Optional.empty();
    }
  }
}
