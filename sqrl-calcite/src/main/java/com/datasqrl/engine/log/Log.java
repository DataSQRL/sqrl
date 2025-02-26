package com.datasqrl.engine.log;

import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

public interface Log {

  TableSource getSource();

  TableSink getSink();

  IConnectorFactoryContext getConnectorContext();

  RelDataType getSchema();

  @Value
  class Impl {

    TableSource source;
    TableSink sink;
  }
}
