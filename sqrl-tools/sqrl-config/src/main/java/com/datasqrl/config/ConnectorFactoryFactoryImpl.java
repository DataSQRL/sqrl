package com.datasqrl.config;

import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;

/**
 * Placeholder for future templated connector handling
 */
@AllArgsConstructor(onConstructor_=@Inject)
public class ConnectorFactoryFactoryImpl implements ConnectorFactoryFactory {

  PackageJson packageJson;

  private Optional<ConnectorConf> getConnectorConfig(String connectorName) {
    var engineConfig = packageJson.getEngines().getEngineConfig("flink");
    Preconditions.checkArgument(engineConfig.isPresent(), "Missing engine configuration for Flink");
    var connectors = engineConfig.get().getConnectors();
    return connectors.getConnectorConfig(connectorName);
  }


  @Override
  public Optional<ConnectorFactory> create(SystemBuiltInConnectors builtInConnector) {

    return switch (builtInConnector) {
    case PRINT_SINK -> Optional.of(createPrintConnectorFactory(null));
    case LOCAL_FILE_SOURCE -> getConnectorConfig(builtInConnector.getName().getCanonical()).map(this::createLocalFile);
    default -> throw new IllegalArgumentException("Unknown connector: " + builtInConnector);
    };
  }

  @Override
  public Optional<ConnectorFactory> create(EngineType engineType, String connectorName) {

    // from conflict, include into the new logic
    var engineConfig = packageJson.getEngines().getEngineConfig("flink");
    Preconditions.checkArgument(engineConfig.isPresent(), "Missing engine configuration for Flink");
    var connectors = engineConfig.get().getConnectors();


    //TODO: Move the rest of this method into the respective engines

    if (connectorName.equalsIgnoreCase("iceberg")) { //work around until we get the correct engine in
      return connectors.getConnectorConfig("iceberg").map(this::createIceberg);
    }
    // end

    var connectorConfig = getConnectorConfig(connectorName);
    if (connectorName.equals("postgres_log-source") || connectorName.equals("postgres_log-sink")) {
      return connectorConfig.map(this::createPostgresLogConnectorFactory);
    }

    if (engineType != null) {
      if (engineType.equals(EngineType.LOG)) {
        return connectorConfig.map(this::createKafkaConnectorFactory);
      } else if (engineType.equals(EngineType.DATABASE)) {
        return connectorConfig.map(this::createJdbcConnectorFactory);
      } else if (connectorName.equalsIgnoreCase("iceberg")) {
        return connectorConfig.map(this::createIceberg);
      } else {
        throw new IllegalArgumentException("Unable to create connectorConfig for engineType=" + engineType.name());
      }
    }

    throw new RuntimeException("Connector not supported: " + connectorName);
  }

  @Override
  public ConnectorConf getConfig(String name) {
    var connectors = packageJson.getEngines().getEngineConfigOrErr("flink")
        .getConnectors();
    return connectors.getConnectorConfigOrErr(name);
  }

  @Override
  public Optional<ConnectorConf> getOptionalConfig(String name) {
    var connectors = packageJson.getEngines().getEngineConfigOrErr("flink")
        .getConnectors();
    return connectors.getConnectorConfig(name);
  }

  //## TODO: move the rest of this class into the respective engines

  private ConnectorFactory createIceberg(ConnectorConf connectorConf) {
    // todo template this
    return context -> {
      var map = connectorConf.toMap();
      var builder = TableConfigImpl.builder(context.getName());
      map.entrySet().forEach(e->
          builder.getConnectorConfig().setProperty(e.getKey(), e.getValue()));
      builder.getConnectorConfig().setProperty("catalog-table", context.getName());

      builder.setType(ExternalDataType.source_and_sink);
      return builder.build();
    };
  }

  private ConnectorFactory createPrintConnectorFactory(ConnectorConf connectorConf) {

    return context -> {
      var name = (String) context.getMap().get("name");
      var builder = TableConfigImpl.builder(context.getName());
      builder.setType(ExternalDataType.sink);
      builder.getConnectorConfig().setProperty("connector", "print");
      builder.getConnectorConfig().setProperty("print-identifier", name);
      return builder.build();
    };
  }

  private ConnectorFactory createLocalFile(ConnectorConf connectorConf) {

    return context -> {
      var contextData = context.getMap();

      var filename = (String)contextData.get("filename");
      var format = (String)contextData.get("format");
      var primaryKey = (String[])contextData.get("primary-key");
      var builder = TableConfigImpl.builder(context.getName());
      builder.setType(ExternalDataType.source);
      builder.setPrimaryKey(primaryKey);
      builder.setMetadataFunction("_ingest_time", "proctime()");
      builder.setTimestampColumn("_ingest_time");

      var engineConfig = (ConnectorConfImpl) connectorConf;
      builder.copyConnectorConfig(engineConfig);
      builder.getConnectorConfig().setProperty("path", "${DATA_PATH}/" + filename);
      builder.getConnectorConfig().setProperty("format", format);

      return builder.build();
    };


  }

  private ConnectorFactory createKafkaConnectorFactory(ConnectorConf connectorConf) {

    return context -> {
      var map = context.getMap();
      var connectorConf1 = (ConnectorConfImpl) connectorConf;

      var builder = TableConfigImpl.builder(context.getName());
      var primaryKey = (List<String>)map.get("primary-key");
      var timestampType = (String)map.get("timestamp-type");
      var timestampName = (String)map.get("timestamp-name");

      if (primaryKey != null && !primaryKey.isEmpty()) {
        builder.setPrimaryKey(primaryKey.toArray(new String[0]));
    }
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
      var builder = TableConfigImpl.builder(context.getName());
      builder.setType(ExternalDataType.sink);

      var engineConfig = (ConnectorConfImpl) connectorConf;
      var map = context.getMap();
      builder.copyConnectorConfig(engineConfig);
      builder.getConnectorConfig().setProperty("table-name", map.get("table-name"));
      builder.getConnectorConfig().setProperty("connector", "jdbc-sqrl");

      return builder.build();
    };
  }

  private ConnectorFactory createPostgresLogConnectorFactory(ConnectorConf connectorConf) {

    return context -> {
      var map = context.getMap();
      var engineConfig = (ConnectorConfImpl) connectorConf;

      var builder = TableConfigImpl.builder(context.getName());

      var primaryKey = (List<String>)map.get("primary-key");
      Optional.ofNullable(primaryKey)
          .filter(pk -> !pk.isEmpty())
          .ifPresent(pk -> builder.setPrimaryKey(pk.toArray(new String[0])));

      var timestampName = (String) map.get("timestamp-name");
      if (timestampName == null) {
        timestampName = "event_time";
    }
      builder.setTimestampColumn(timestampName);
      builder.setWatermark(0);


      if (connectorConf.toMap().get("connector").equals("postgres-cdc")) {
        builder.setType(ExternalDataType.source);
      } else {
        builder.setType(ExternalDataType.sink);
      }

      builder.copyConnectorConfig(engineConfig);
      builder.getConnectorConfig().setProperty("table-name", map.get("table-name"));
      return builder.build();
    };
  }

}
