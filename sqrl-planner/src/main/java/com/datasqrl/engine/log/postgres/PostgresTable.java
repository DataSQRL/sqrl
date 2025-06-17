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
// package com.datasqrl.engine.log.postgres;
//
// import java.util.List;
// import java.util.Optional;
//
// import org.apache.calcite.rel.type.RelDataType;
//
// import com.datasqrl.canonicalizer.Name;
// import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
// import com.datasqrl.config.TableConfig;
// import com.datasqrl.engine.log.Log;
// import com.datasqrl.io.tables.TableSink;
// import com.datasqrl.io.tables.TableSinkImpl;
// import com.datasqrl.io.tables.TableSource;
// import com.datasqrl.plan.table.RelDataTypeTableSchema;
// import com.google.common.base.Preconditions;
//
// import lombok.Getter;
//
// @Getter
// public class PostgresTable implements Log {
//
//  String tableName;
//  Name logName;
//  TableConfig sourceConfig;
//  TableConfig sinkConfig;
//  RelDataTypeTableSchema tableSchema;
//  List<String> primaryKeys;
//  IConnectorFactoryContext connectorContext;
//
//  public PostgresTable(String tableName, Name logName, TableConfig sourceConfig,
//      TableConfig sinkConfig, RelDataTypeTableSchema tableSchema, List<String> primaryKeys,
//      IConnectorFactoryContext connectorContext) {
//    this.tableName = tableName;
//    this.logName = logName;
//    this.sourceConfig = sourceConfig;
//    this.sinkConfig = sinkConfig;
//    this.tableSchema = tableSchema;
//    Preconditions.checkState(!primaryKeys.isEmpty(), "Postgres table should have primary keys");
//    this.primaryKeys = primaryKeys;
//    this.connectorContext = connectorContext;
//  }
//
//  @Override
//  public TableSource getSource() {
//    return TableSource.create(sourceConfig, logName.toNamePath(), tableSchema);
//  }
//
//  @Override
//  public TableSink getSink() {
//    return TableSinkImpl.create(sinkConfig, logName.toNamePath(), Optional.of(tableSchema));
//  }
//
//  @Override
//  public RelDataType getSchema() {
//    return tableSchema.getRelDataType();
//  }
//
// }
