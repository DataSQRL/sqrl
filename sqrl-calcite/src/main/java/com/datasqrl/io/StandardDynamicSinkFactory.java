package com.datasqrl.io;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.tables.FlinkConnectorFactory;
import com.datasqrl.io.tables.TableConfig;
import lombok.Value;

@Value
public class StandardDynamicSinkFactory implements DynamicSinkFactory {

  DynamicSinkConnectorFactory connectorFactory;
  SqrlConfig baseConnectorConfig;

  @Override
  public TableConfig get(Name name) {
    TableConfig.Builder builder = TableConfig.builder(name);
    builder.setType(ExternalDataType.sink);
    builder.copyConnectorConfig(connectorFactory.forName(name, baseConnectorConfig));
    return builder.build();
  }

  public static StandardDynamicSinkFactory of(TableConfig tableConfig) {
    return tableConfig.getConnectorConfig().asDynamicSink();
  }
}
