//package com.datasqrl.engine.log.postgres;
//
//import java.util.List;
//import java.util.Optional;
//
//import org.apache.calcite.rel.type.RelDataType;
//
//import com.datasqrl.canonicalizer.Name;
//import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
//import com.datasqrl.config.TableConfig;
//import com.datasqrl.engine.log.Log;
//import com.datasqrl.io.tables.TableSink;
//import com.datasqrl.io.tables.TableSinkImpl;
//import com.datasqrl.io.tables.TableSource;
//import com.datasqrl.plan.table.RelDataTypeTableSchema;
//import com.google.common.base.Preconditions;
//
//import lombok.Getter;
//
//@Getter
//public class PostgresTable implements Log {
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
//}
