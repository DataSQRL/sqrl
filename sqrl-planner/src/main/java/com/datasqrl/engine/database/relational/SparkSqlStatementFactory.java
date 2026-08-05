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
import com.datasqrl.engine.database.relational.ddl.GenericCreateTableDdlFactory;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.hint.DataTypeHint;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

public class SparkSqlStatementFactory extends AbstractJdbcStatementFactory {

  public SparkSqlStatementFactory() {
    super(Dialect.SPARK_SQL, new GenericCreateTableDdlFactory(ExtendedSparkSqlDialect.DEFAULT));
  }

  @Override
  protected SqlNode getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    return ExtendedSparkSqlDialect.DEFAULT.getCastSpec(type);
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("Spark SQL does not support indexes");
  }
}
