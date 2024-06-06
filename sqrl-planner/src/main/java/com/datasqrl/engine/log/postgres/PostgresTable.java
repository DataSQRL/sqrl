package com.datasqrl.engine.log.postgres;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.log.Log;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSinkImpl;
import com.datasqrl.io.tables.TableSource;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class PostgresTable implements Log {

  String topicName;
  Name logName;
  TableConfig logConfig;
  Optional<TableSchema> tableSchema;
  IConnectorFactoryContext connectorContext;

  @Override
  public TableSource getSource() {
    return TableSource.create(logConfig, logName.toNamePath(), tableSchema.get());
  }

  @Override
  public TableSink getSink() {
    return TableSinkImpl.create(logConfig, logName.toNamePath(), tableSchema);
  }

}
