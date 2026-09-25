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

import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_CATALOG;
import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_DATABASE;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_DATABASE_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_IMPL_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_TABLE_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_GLUE_CATALOG_IMPL;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.relational.ddl.GenericCreateTableDdlFactory;
import com.datasqrl.engine.database.relational.ddl.GenericCreateViewDdlFactory;
import com.datasqrl.engine.database.relational.ddl.RedshiftCreateViewDdlFactory;
import com.datasqrl.engine.database.relational.ddl.ViewIdentifierResolver;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.hint.DataTypeHint;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class RedshiftStatementFactory extends AbstractJdbcStatementFactory {

  private static final String GLUE_CATALOG_NAME = "awsdatacatalog";
  private static final String ICEBERG_CATALOG_NAME_KEY = "catalog-name";

  private final EngineConfig engineConfig;

  public RedshiftStatementFactory(EngineConfig engineConfig) {
    super(Dialect.REDSHIFT, new GenericCreateTableDdlFactory(RedshiftSqlDialect.DEFAULT));
    this.engineConfig = engineConfig;
  }

  @Override
  protected SqlNode getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    return RedshiftSqlDialect.DEFAULT.getCastSpec(type);
  }

  @Override
  protected ViewIdentifierResolver getViewIdentifierResolver() {
    return ViewIdentifierResolver.propertyHierarchy(
        engineConfig, "view-database", "view-schema", "public");
  }

  @Override
  protected GenericCreateViewDdlFactory getCreateViewDdlFactory() {
    return new RedshiftCreateViewDdlFactory(getViewIdentifierResolver());
  }

  @Override
  protected Map<String, SqlIdentifier> getTableNameMapping(
      Map<String, JdbcEngineCreateTable> tableIdMap) {
    return tableIdMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> getSourceTableIdentifier(entry.getValue())));
  }

  private SqlIdentifier getSourceTableIdentifier(JdbcEngineCreateTable table) {
    var connectorOptions = table.table().getConnectorOptions();
    var catalogDatabase =
        connectorOptions.getOrDefault(ICEBERG_CATALOG_DATABASE_KEY, FLINK_DEFAULT_DATABASE);
    var catalogTable = connectorOptions.get(ICEBERG_CATALOG_TABLE_KEY);
    var catalogName =
        ICEBERG_GLUE_CATALOG_IMPL.equals(connectorOptions.get(ICEBERG_CATALOG_IMPL_KEY))
            ? GLUE_CATALOG_NAME
            : connectorOptions.getOrDefault(ICEBERG_CATALOG_NAME_KEY, FLINK_DEFAULT_CATALOG);

    if (catalogTable == null) {
      catalogTable = table.tableName();
    }

    return new SqlIdentifier(
        List.of(catalogName, catalogDatabase, catalogTable), SqlParserPos.ZERO);
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("Redshift does not support indexes");
  }
}
