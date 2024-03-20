package com.datasqrl.io.connector;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.DynamicSinkConnectorFactory;
import com.datasqrl.io.StandardDynamicSinkFactory;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.tables.ConnectorFactory;
import com.datasqrl.io.tables.FlinkConnectorFactory;
import com.datasqrl.io.tables.TableType;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

@AllArgsConstructor
@Getter
public class ConnectorConfig {

  protected final SqrlConfig config;
  protected final ConnectorFactory connectorFactory;

  public Optional<Format> getFormat() {
    return connectorFactory.getFormat(config);
  }

  public TableType getTableType() {
    return connectorFactory.getTableType(config);
  }

  public StandardDynamicSinkFactory asDynamicSink() {
    String connectorName = connectorFactory.getConnectorName(config);
    DynamicSinkConnectorFactory sinkFactory = DynamicSinkConnectorFactory.load(connectorName);
    return new StandardDynamicSinkFactory(sinkFactory, config);
  }


}
