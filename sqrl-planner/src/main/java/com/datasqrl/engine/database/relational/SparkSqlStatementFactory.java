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
package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.dialect.ExtendedSparkSqlDialect;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.relational.ddl.GenericCreateTableDdlFactory;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.hint.DataTypeHint;
import java.util.ArrayList;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SparkSqlStatementFactory extends AbstractJdbcStatementFactory {

  private final EngineConfig engineConfig;

  public SparkSqlStatementFactory(EngineConfig engineConfig) {
    super(Dialect.SPARK_SQL, new GenericCreateTableDdlFactory(ExtendedSparkSqlDialect.DEFAULT));
    this.engineConfig = engineConfig;
  }

  @Override
  protected SqlNode getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    return ExtendedSparkSqlDialect.DEFAULT.getCastSpec(type);
  }

  @Override
  protected SqlIdentifier getViewStatementIdentifier(String viewName) {
    var names = new ArrayList<String>();
    var catalog = engineConfig.getPropertyOptional("view-catalog");
    if (catalog.isPresent()) {
      names.add(catalog.get());
      names.add(engineConfig.getPropertyOptional("view-database").orElse("default"));
    } else {
      engineConfig.getPropertyOptional("view-database").ifPresent(names::add);
    }
    names.add(viewName);
    return new SqlIdentifier(names, SqlParserPos.ZERO);
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("Spark SQL does not support indexes");
  }
}
