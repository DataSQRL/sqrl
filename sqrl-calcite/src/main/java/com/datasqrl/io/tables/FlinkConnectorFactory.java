package com.datasqrl.io.tables;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.formats.Format.BaseFormat;
import com.datasqrl.io.formats.Format.DefaultFormat;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.tables.TableConfig.Builder;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;

public class FlinkConnectorFactory implements ConnectorFactory {

  public static final FlinkConnectorFactory INSTANCE = new FlinkConnectorFactory();

  public static final String CONNECTOR_KEY = "connector";
  public static final String FORMAT_KEY = "format";
  public static final String VALUE_FORMAT_KEY = "value.format";

  public static Map<String,TableType> CONNECTOR_TYPE_MAP = ImmutableMap.of(
      "kafka",TableType.STREAM,
      "file", TableType.STREAM,
      "filesystem", TableType.STREAM,
      "upsert-kafka", TableType.VERSIONED_STATE,
      "jdbc", TableType.LOOKUP,
      "jdbc-sqrl", TableType.LOOKUP
  );

  @Override
  public Format getFormat(SqrlConfig connectorConfig) {
    Optional<String> format = connectorConfig.asString(FORMAT_KEY).getOptional()
        .or(() -> connectorConfig.asString(VALUE_FORMAT_KEY).getOptional());
    connectorConfig.getErrorCollector()
        .checkFatal(format.isPresent(), "Need to configure a format via [%s] or [%s]", FORMAT_KEY,
            VALUE_FORMAT_KEY);
    Optional<FormatFactory> formatFactory = ServiceLoaderDiscovery.findFirst(FormatFactory.class,
        FormatFactory::getName, format.get());
    return formatFactory.map(fac -> fac.fromConfig(connectorConfig))
        .orElseGet(() -> new DefaultFormat(format.get()));
  }

  @Override
  public Optional<Format> getFormatForExtension(String formatExtension) {
    Optional<FormatFactory> formatFactory = ServiceLoaderDiscovery.findFirst(FormatFactory.class,
        factory -> factory.isExtension(formatExtension));
    return formatFactory.map(FormatFactory::createDefault);
  }

  protected void setFormat(SqrlConfig connectorConfig, Format format) {
    connectorConfig.setProperty(FORMAT_KEY, format.getName());
  }

  @Override
  public String getConnectorName(SqrlConfig connectorConfig) {
    return connectorConfig.asString(CONNECTOR_KEY).get();
  }

  @Override
  public TableType getTableType(SqrlConfig connectorConfig) {
    String connectorName = getConnectorName(connectorConfig).toLowerCase();
    TableType tableType = CONNECTOR_TYPE_MAP.get(connectorName);
    if (tableType == null) {
      connectorConfig.getErrorCollector()
          .warn("Unrecognized connector type: %s. Defaulting to STREAM table for import.",
              connectorName);
      tableType = TableType.STREAM;
    }
    return tableType;
  }


}
