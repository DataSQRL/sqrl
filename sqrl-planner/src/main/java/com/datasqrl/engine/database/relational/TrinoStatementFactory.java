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
import com.datasqrl.calcite.dialect.ExtendedTrinoSqlDialect;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.deployment.model.JdbcStatementModel.Field;
import com.datasqrl.engine.database.relational.ddl.TrinoCreateTableDdlFactory;
import com.datasqrl.engine.database.relational.ddl.TrinoCreateViewDdlFactory;
import com.datasqrl.engine.database.relational.ddl.TrinoTypeFormatter;
import com.datasqrl.engine.database.relational.ddl.ViewIdentifierResolver;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.hint.DataTypeHint;
import com.datasqrl.planner.hint.PlannerHints;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public class TrinoStatementFactory extends AbstractJdbcStatementFactory {
  private final EngineConfig engineConfig;

  public TrinoStatementFactory(EngineConfig engineConfig) {
    super(Dialect.TRINO, new TrinoCreateTableDdlFactory());
    this.engineConfig = engineConfig;
  }

  @Override
  protected TrinoCreateViewDdlFactory getCreateViewDdlFactory() {
    return new TrinoCreateViewDdlFactory(getViewIdentifierResolver());
  }

  @Override
  protected ViewIdentifierResolver getViewIdentifierResolver() {
    return ViewIdentifierResolver.propertyHierarchy(
        engineConfig, "view-catalog", "view-schema", "public");
  }

  @Override
  protected SqlNode getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    return ExtendedTrinoSqlDialect.DEFAULT.getCastSpec(type);
  }

  @Override
  protected Field toField(RelDataTypeField field, PlannerHints hints, Documentation documentation) {
    return new Field(
        field.getName(),
        TrinoTypeFormatter.format(field.getType()),
        field.getType().isNullable(),
        documentation.getColumn(field.getName(), null));
  }

  @Override
  protected String createView(SqlIdentifier viewName, SqlNodeList columns, SqlNode query) {
    return getCreateViewDdlFactory().createView(viewName, sqlConverters.convert(query));
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("Trino does not support indexes");
  }
}
