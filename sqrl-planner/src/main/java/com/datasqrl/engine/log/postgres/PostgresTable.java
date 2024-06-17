package com.datasqrl.engine.log.postgres;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.log.Log;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSinkImpl;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class PostgresTable implements Log {

  String topicName;
  Name logName;
  TableConfig sourceConfig;
  TableConfig sinkConfig;
  RelDataTypeTableSchema tableSchema;
  IConnectorFactoryContext connectorContext;

  @Override
  public TableSource getSource() {
    return TableSource.create(sourceConfig, logName.toNamePath(), tableSchema);
  }

  @Override
  public TableSink getSink() {
    return TableSinkImpl.create(sinkConfig, logName.toNamePath(), Optional.of(tableSchema));
  }

}