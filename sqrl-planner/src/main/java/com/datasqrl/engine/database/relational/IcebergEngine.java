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

import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_IMPL_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_TABLE_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_GLUE_CATALOG_IMPL;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.iceberg.IcebergDataTypeMapper;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.google.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.NonNull;

public class IcebergEngine extends AbstractJDBCTableFormatEngine {

  private static final Pattern GLUE_TABLE_PATTERN = Pattern.compile("^[a-z0-9_]{1,255}$");

  @Inject
  public IcebergEngine(@NonNull PackageJson json, ConnectorFactoryFactory connectorFactory) {
    super(
        IcebergEngineFactory.ENGINE_NAME,
        json.getEngines().getEngineConfigOrEmpty(IcebergEngineFactory.ENGINE_NAME),
        connectorFactory);
  }

  @Override
  public boolean supportsQueryEngine(QueryEngine engine) {
    return engine instanceof SnowflakeEngine || engine instanceof DuckDBEngine;
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Iceberg;
  }

  @Override
  public String getConnectorTableName(FlinkTableBuilder tableBuilder) {
    return tableBuilder.getConnectorOptions().get(ICEBERG_CATALOG_TABLE_KEY);
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new IcebergStatementFactory();
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    return new IcebergDataTypeMapper();
  }

  @Override
  protected Map<String, String> getConnectorOptions(String originalTableName, String tableId) {
    var connectorOptions = super.getConnectorOptions(originalTableName, tableId);

    if (!isGlueCatalog(connectorOptions)) {
      return connectorOptions;
    }

    var tableName = connectorOptions.get(ICEBERG_CATALOG_TABLE_KEY);
    tableName = validatedGlueTableName(tableName);

    var mutableOptions = new HashMap<>(connectorOptions);
    mutableOptions.put(ICEBERG_CATALOG_TABLE_KEY, tableName);

    return mutableOptions;
  }

  private boolean isGlueCatalog(Map<String, String> connectorOptions) {
    var catalogImpl = connectorOptions.get(ICEBERG_CATALOG_IMPL_KEY);

    return ICEBERG_GLUE_CATALOG_IMPL.equals(catalogImpl);
  }

  /** AWS GlueCatalog names have to match {@code GLUE_TABLE_PATTERN}. */
  private String validatedGlueTableName(String tableName) {
    var loweredTableName = tableName.toLowerCase();

    if (GLUE_TABLE_PATTERN.matcher(loweredTableName).matches()) {
      return loweredTableName;
    }

    throw new IllegalArgumentException(
        "Invalid AWS Glue table name: '%s'. Name has to match: %s"
            .formatted(loweredTableName, GLUE_TABLE_PATTERN));
  }
}
