package com.datasqrl.engine.log.postgres;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.log.Log;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSinkImpl;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

@AllArgsConstructor
@Getter
public class PostgresTable implements Log {

  String tableName;
  Name logName;
  TableConfig sourceConfig;
  TableConfig sinkConfig;
  RelDataTypeTableSchema tableSchema;
  List<String> primaryKeys;
  IConnectorFactoryContext connectorContext;
  RelDataType schema;

  @Override
  public TableSource getSource() {
    return TableSource.create(sourceConfig, logName.toNamePath(), tableSchema);
  }

  @Override
  public TableSink getSink() {
    return TableSinkImpl.create(sinkConfig, logName.toNamePath(), Optional.of(tableSchema));
  }

}
