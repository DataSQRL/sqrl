package com.datasqrl.config;

import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.TableConfig.Format;
import com.google.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

/**
 * Placeholder for future templated connector handling
 */
@AllArgsConstructor(onConstructor_=@Inject)
public class ConnectorFactoryFactoryImpl implements ConnectorFactoryFactory {

  PackageJson packageJson;
  @Override
  public Optional<ConnectorFactory> create(Type type, String name) {
    ConnectorsConfig connectors = packageJson.getEngines().getEngineConfig("flink")
        .get().getConnectors();
    Optional<ConnectorConf> connectorConfig = connectors.getConnectorConfig(name);
    if (name.equalsIgnoreCase(PRINT_SINK_NAME)) {
      return Optional.of(createPrintConnectorFactory(null));
    }
    if (type.equals(Type.LOG)) {
      return Optional.of(createKafkaConnectorFactory(connectorConfig.get()));
    } else if (type.equals(Type.DATABASE)) {
      return Optional.of(createJdbcConnectorFactory(connectorConfig.get()));
    }

    return connectorConfig.map(c -> context -> null);
  }

  @Override
  public ConnectorConf getConfig(String name) {
    ConnectorsConfig connectors = packageJson.getEngines().getEngineConfig("flink")
        .get().getConnectors();
    Optional<ConnectorConf> connectorConfig = connectors.getConnectorConfig(name);
    return connectorConfig.get();
  }

  private ConnectorFactory createPrintConnectorFactory(ConnectorConf engineConfig) {

    return context -> {
      TableConfigBuilderImpl builder = TableConfigImpl.builder(
          (String) context.getMap().get("name"));
      builder.setType(ExternalDataType.sink);
      builder.getConnectorConfig().setProperty("connector", PRINT_SINK_NAME);
      builder.getConnectorConfig().setProperty("print-identifier", (String) context.getMap().get("name"));
      return builder.build();
    };
  }

  private ConnectorFactory createKafkaConnectorFactory(ConnectorConf connectorConf) {

    return context -> {
      Map<String, Object> map = context.getMap();
      ConnectorConfImpl connectorConf1 = (ConnectorConfImpl) connectorConf;

//      String topicName = sanitizeName(logId);
      TableConfigBuilderImpl builder = TableConfigImpl.builder((String)map.get("topic"));
      List<String> primaryKey = (List<String>)map.get("primary-key");
      String timestampType = (String)map.get("timestamp-type");
      String timestampName = (String)map.get("timestamp-name");
//
      if (!primaryKey.isEmpty()) builder.setPrimaryKey(primaryKey.toArray(new String[0]));
      if (!timestampType.equalsIgnoreCase("NONE")) {//!=TimestampType.NONE
        builder.setType(ExternalDataType.source_and_sink);
        builder.setTimestampColumn(timestampName);
        builder.setWatermark(1);
        if (timestampType.equalsIgnoreCase("LOG_TIME")) {
          builder.setMetadata(timestampName, "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)",
              "timestamp"); //todo fix?
//                  connectorFactory.getEventTime());
        } else {
          throw new UnsupportedOperationException("Not yet supported: " + timestampType);
        }
      } else {
        builder.setType(ExternalDataType.sink);
      }

      builder.copyConnectorConfig(connectorConf1);
      builder.getConnectorConfig().setProperty("topic", map.get("topic"));
      return builder.build();
    };
  }

  private ConnectorFactory createJdbcConnectorFactory(ConnectorConf engineConfig1) {
    return context -> {
      Map<String, Object> map = context.getMap();

      String tableName = (String)map.get("table-name");
      TableConfigBuilderImpl builder = TableConfigImpl.builder(tableName);
      builder.setType(ExternalDataType.sink);

      ConnectorConfImpl engineConfig = (ConnectorConfImpl) engineConfig1;
      builder.copyConnectorConfig(engineConfig);
      builder.getConnectorConfig().setProperty("table-name", tableName);
      builder.getConnectorConfig().setProperty("connector", "jdbc-sqrl");

      return builder.build();
    };
  }
}
