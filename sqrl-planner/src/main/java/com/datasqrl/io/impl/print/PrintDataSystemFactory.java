package com.datasqrl.io.impl.print;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.DataSystemDiscoveryFactory;
import com.datasqrl.io.DataSystemImplementationFactory;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FormatFactoryOld;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.tables.TableConfig;
import com.google.auto.service.AutoService;
import lombok.NonNull;

public abstract class PrintDataSystemFactory implements DataSystemImplementationFactory {

  public static final String SYSTEM_NAME = "print";

  @Override
  public String getSystemName() {
    return SYSTEM_NAME;
  }

  @AutoService(DataSystemConnectorFactory.class)
  public static class Connector extends PrintDataSystemFactory
      implements DataSystemConnectorFactory {

    @Override
    public DataSystemConnectorSettings getSettings(@NonNull SqrlConfig connectorConfig) {
      return DataSystemConnectorSettings.builder().hasSourceTimestamp(false).build();
    }

  }

  @AutoService(DataSystemDiscoveryFactory.class)
  public static class Discovery extends PrintDataSystemFactory implements
      DataSystemDiscoveryFactory {

    @Override
    public DataSystemDiscovery initialize(@NonNull TableConfig tableConfig) {
      return new PrintDataSystemDiscovery(tableConfig);
    }


  }

  public static TableConfig getDefaultDiscoveryConfig() {
    TableConfig.Builder builder = TableConfig.builder(SYSTEM_NAME);
//    builder.base(BaseTableConfig.builder()
//        .type(ExternalDataType.sink.name())
//        .build());
    builder.getFormatConfig().setProperty(FormatFactoryOld.FORMAT_NAME_KEY, "json");
    builder.getConnectorConfig().setProperty(SYSTEM_NAME_KEY, SYSTEM_NAME);
    return builder.build();
  }

}
