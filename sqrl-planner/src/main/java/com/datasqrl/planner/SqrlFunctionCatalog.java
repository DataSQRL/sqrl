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
package com.datasqrl.planner;

import com.datasqrl.planner.tables.SqrlTableFunction;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;

/**
 * A simple wrapper for Apache Calcite's schema/catalog so we can register SqrlTableFunctions as
 * table functions and have them be accessible to the Flink parser.
 */
public class SqrlFunctionCatalog {

  CalciteSchema schema;
  SchemaPlus schemaPlus;
  CalciteCatalogReader catalogReader;

  public SqrlFunctionCatalog(RelDataTypeFactory typeFactory) {
    schema = CalciteSchema.createRootSchema(false, false);
    schemaPlus = schema.plus();
    var info = new Properties();
    info.setProperty("caseSensitive", "false");
    var config = new CalciteConnectionConfigImpl(info);
    this.catalogReader = new CalciteCatalogReader(schema, List.of(), typeFactory, config);
  }

  public void addFunction(SqrlTableFunction function) {
    Preconditions.checkArgument(
        schemaPlus.getFunctions(function.getFunctionCatalogName()).isEmpty(),
        "Function [%s] already exists",
        function.getFunctionCatalogName());
    schemaPlus.add(function.getFunctionCatalogName(), function);
  }

  public SqlOperatorTable getOperatorTable() {
    return catalogReader;
  }
}
