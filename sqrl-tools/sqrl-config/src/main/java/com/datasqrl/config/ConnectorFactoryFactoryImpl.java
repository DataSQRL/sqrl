package com.datasqrl.config;

import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.google.common.base.Preconditions;
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
    Optional<EngineConfig> engineConfig = packageJson.getEngines().getEngineConfig("flink");
    Preconditions.checkArgument(engineConfig.isPresent(), "Missing engine configuration for Flink");
    ConnectorsConfig connectors = engineConfig.get().getConnectors();

    if (name.equalsIgnoreCase(LOG_SINK_NAME)) {
      switch (packageJson.getLogMethod()) {
        case PRINT:
          return Optional.of(createPrintConnectorFactory(null));
        case LOG_ENGINE:
          List<String> engines = packageJson.getEnabledEngines();
          if (engines.contains("kafka")) {
            return connectors.getConnectorConfig("kafka").map(this::createKafkaConnectorFactory);
//          } else if (engines.contains("postgres-log")) {
//            return connectors.getConnectorConfig("postgres-log-sink").map(this::createPostgresLogExportConnectionFactory);
          } else {
            throw new IllegalArgumentException("Only the Kafka and Postgres-log engines are supported for use as log sinks.");
          }
        case NONE:
          return Optional.of(createBlackHoleConnectorFactory());
      }
    }

    Optional<ConnectorConf> connectorConfig = connectors.getConnectorConfig(name);
    if (name.equalsIgnoreCase(PRINT_SINK_NAME)) { // TODO: to deprecate it?
      return Optional.of(createPrintConnectorFactory(null));
    } else if (name.equalsIgnoreCase(FILE_SINK_NAME)) {
      return connectorConfig.map(this::createLocalFile);
    }

    if (type != null) {
      if (type.equals(Type.LOG)) {
        return connectorConfig.map(this::createKafkaConnectorFactory);
      } else if (type.equals(Type.DATABASE)) {
        return connectorConfig.map(this::createJdbcConnectorFactory);
      }
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

  private ConnectorFactory createPrintConnectorFactory(ConnectorConf connectorConf) {

    return context -> {
      String name = (String) context.getMap().get("name");
      TableConfigBuilderImpl builder = TableConfigImpl.builder(context.getName());
      builder.setType(ExternalDataType.sink);
      builder.getConnectorConfig().setProperty("connector", PRINT_SINK_NAME);
      builder.getConnectorConfig().setProperty("print-identifier", name);
      return builder.build();
    };
  }

  private ConnectorFactory createLocalFile(ConnectorConf connectorConf) {

    return context -> {
      Map<String, Object> contextData = context.getMap();

      String filename = (String)contextData.get("filename");
      String format = (String)contextData.get("format");
      TableConfigBuilderImpl builder = TableConfigImpl.builder(context.getName());
      builder.setType(ExternalDataType.source);
      builder.addUuid("_uuid");
      builder.setPrimaryKey(new String[]{"_uuid"});
      builder.addIngestTime("_ingest_time");
      builder.setTimestampColumn("_ingest_time");

      ConnectorConfImpl engineConfig = (ConnectorConfImpl) connectorConf;
      builder.copyConnectorConfig(engineConfig);
      builder.getConnectorConfig().setProperty("path", "/data/" + filename);
      builder.getConnectorConfig().setProperty("format", format);

      return builder.build();
    };


  }

  private ConnectorFactory createKafkaConnectorFactory(ConnectorConf connectorConf) {

    return context -> {
      Map<String, Object> map = context.getMap();
      ConnectorConfImpl connectorConf1 = (ConnectorConfImpl) connectorConf;

//      String topicName = sanitizeName(logId);
      TableConfigBuilderImpl builder = TableConfigImpl.builder(context.getName());
      List<String> primaryKey = (List<String>)map.get("primary-key");
      String timestampType = (String)map.get("timestamp-type");
      String timestampName = (String)map.get("timestamp-name");
//
      if (primaryKey != null && !primaryKey.isEmpty()) builder.setPrimaryKey(primaryKey.toArray(new String[0]));
      if (timestampType != null && !timestampType.equalsIgnoreCase("NONE")) {//!=TimestampType.NONE
        builder.setType(ExternalDataType.source_and_sink);
        builder.setTimestampColumn(timestampName);
        builder.setWatermark(0);
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

  private ConnectorFactory createJdbcConnectorFactory(ConnectorConf connectorConf) {
    return context -> {
      TableConfigBuilderImpl builder = TableConfigImpl.builder(context.getName());
      builder.setType(ExternalDataType.sink);

      ConnectorConfImpl engineConfig = (ConnectorConfImpl) connectorConf;
      Map<String, Object> map = context.getMap();
      builder.copyConnectorConfig(engineConfig);
      builder.getConnectorConfig().setProperty("table-name", (String)map.get("table-name"));
      builder.getConnectorConfig().setProperty("connector", "jdbc-sqrl");

      return builder.build();
    };
  }

  private ConnectorFactory createBlackHoleConnectorFactory() {
    return context -> {
      TableConfigBuilderImpl builder = TableConfigImpl.builder(context.getName());
      builder.setType(ExternalDataType.sink);
      builder.getConnectorConfig().setProperty("connector", "blackhole");

      return builder.build();
    };
  }
}
