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
import com.datasqrl.calcite.OperatorRuleTransformer;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.ddl.IcebergCreateTableDdlFactory;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.hint.DataTypeHint;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;

public class IcebergStatementFactory extends AbstractJdbcStatementFactory {

  public IcebergStatementFactory() {
    super(
        new OperatorRuleTransformer(Dialect.POSTGRES),
        null, // Iceberg does not support queries
        null, // Iceberg does not support queries
        new IcebergCreateTableDdlFactory());
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    // TODO: Need to create Iceberg cast spec
    return ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(type);
  }

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  public boolean supportsQueries() {
    return false;
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("Iceberg does not support indexes");
  }
}
