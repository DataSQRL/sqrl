package com.datasqrl.config;

import static com.datasqrl.config.ConnectorConfigImpl.FORMAT_KEY;
import static com.datasqrl.config.ConnectorConfigImpl.VALUE_FORMAT_KEY;

import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.List;
import java.util.Map;
import java.util.Optional;
/**
 * Placeholder for future templated connector handling
 */
public class ConnectorFactoryFactoryImpl implements ConnectorFactoryFactory {

  @Override
  public ConnectorFactory create(String engineId, PackageJson.EngineConfig engineConfig) {
    if (engineId.equalsIgnoreCase("log")) {
      return createKafkaConnectorFactory(engineConfig);
    } else if (engineId.equalsIgnoreCase("database")) {
      return createJdbcConnectorFactory(engineConfig);
    } else if (engineId.equalsIgnoreCase("print")) {
      return createPrintConnectorFactory(engineConfig);
    } else if (engineId.equalsIgnoreCase("file")) {
      return createFileConnectorFactory(engineConfig);
    }

    return new EngineConnectorFactoryImpl(engineConfig);
  }

  private ConnectorFactory createFileConnectorFactory(PackageJson.EngineConfig engineConfig) {

//  @Override
//  public IConnectorConfig forName(@NonNull Name name, @NonNull IEngineConfig baseConnectorConfig) {
////    SqrlConfig connector = SqrlConfig.create(baseConnectorConfig);
////    connector.setProperty(CONNECTOR_KEY, FileFlinkConnectorFactory.CONNECTOR_TYPE);
////    connector.setProperty(FileFlinkConnectorFactory.PATH_KEY,
////        appendDirectory(baseConnectorConfig.asString(FileFlinkConnectorFactory.PATH_KEY).get(), name.getCanonical()));
////    return new ConnectorConfig(connector,this);
//    throw new RuntimeException("");
//
//  }
//
//  public static String appendDirectory(String base, String directory) {
//    String baseStr = base;
//    if (!baseStr.endsWith("/")) baseStr += baseStr + "/";
//    return baseStr + directory;
//  }
//
//  @Override
//  public String getType() {
//    return FileFlinkConnectorFactory.CONNECTOR_TYPE;
//  }
//
//  /**
//   * This method is used for testing only
//   * @param path
//   * @return
//   */
//  public static StandardDynamicSinkFactory forPath(Path path) {
////    SqrlConfig connector = SqrlConfig.createCurrentVersion();
////    connector.setProperty(CONNECTOR_KEY, FileFlinkConnectorFactory.CONNECTOR_TYPE);
////    connector.setProperty(FORMAT_KEY, "json");
////    connector.setProperty(FileFlinkConnectorFactory.PATH_KEY, FilePath.fromJavaPath(path).toString());
////    return new StandardDynamicSinkFactory(new FileFlinkDynamicSinkConnectorFactory(), connector);
//    throw new RuntimeException("");
//
//  }
//todo source
    //
//  public static final int DEFAULT_MONITORING_INTERVAL_MS = 10000;
//
//  public static final String CONNECTOR_TYPE = "filesystem";
//  public static final String PATH_KEY = "path";
//  public static final String REGEX_KEY = "source.path.regex-pattern";
//
//  public static final String MONITOR_INTERVAL_KEY = "source.monitor-interval";
//
//  public IConnectorConfig forFiles(FilePath directory, String fileRegex, Format format) {
////    SqrlConfig connector = SqrlConfig.createCurrentVersion();
////    connector.setProperty(CONNECTOR_KEY, CONNECTOR_TYPE);
////    connector.setProperty(PATH_KEY, directory.toString());
////    connector.setProperty(REGEX_KEY, fileRegex);
////    super.setFormat(connector, format);
////    updateMonitorInterval(connector, DEFAULT_MONITORING_INTERVAL_MS);
////    return new ConnectorConfig(connector, this);
//    throw new RuntimeException();
//  }
//
////  private void updateMonitorInterval(SqrlConfig config, int interval_milliseconds) {
////    config.setProperty(MONITOR_INTERVAL_KEY, interval_milliseconds);
////  }
    return null;
  }

  private ConnectorFactory createPrintConnectorFactory(PackageJson.EngineConfig engineConfig) {

//  public static final String CONNECTOR_TYPE = "print";
//  public static final String PRINT_IDENTIFIER_KEY = "print-identifier";
//
//  @Override
//  public IConnectorConfig forName(@NonNull Name name, @NonNull IEngineConfig baseConnectorConfig) {
////    SqrlConfig connector = SqrlConfig.createCurrentVersion();
////    connector.setProperty(CONNECTOR_KEY, getType());
////    connector.setProperty(PRINT_IDENTIFIER_KEY, name.getDisplay());
////    return new ConnectorConfig(connector, this);
//    throw new RuntimeException("");
//  }
//
//  @Override
//  public String getType() {
//    return CONNECTOR_TYPE;
//  }
    return null;
  }

  @Override
  public Optional<TableConfig.Format> getFormatForExtension(String format) {
    throw new RuntimeException("TBD");
  }

  private ConnectorFactory createKafkaConnectorFactory(PackageJson.EngineConfig engineConfig1) {
    EngineConfigImpl engineConfig = (EngineConfigImpl) engineConfig1;

    return new ConnectorFactory() {
      @Override
      public TableConfig createSourceAndSink(IConnectorFactoryContext context) {
        Map<String, Object> map = context.getMap();


        SqrlConfig config = SqrlConfig.create(engineConfig.sqrlConfig);

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

        builder.copyConnectorConfig(engineConfig);
        builder.getConnectorConfig().setProperty("topic", map.get("topic"));
        return builder.build();
      }

      @Override
      public Optional<TableConfig.Format> getFormat() {
        Optional<String> format = engineConfig.sqrlConfig.asString(FORMAT_KEY).getOptional()
            .or(() -> engineConfig.sqrlConfig.asString(VALUE_FORMAT_KEY).getOptional());
        engineConfig.sqrlConfig.getErrorCollector()
            .checkFatal(format.isPresent(), "Need to configure a format via [%s] or [%s]", FORMAT_KEY,
                VALUE_FORMAT_KEY);
        Optional<FormatFactory> formatFactory = ServiceLoaderDiscovery.findFirst(FormatFactory.class,
            FormatFactory::getName, format.get());
        Optional<TableConfig.Format> format1 = formatFactory.map(fac -> fac.fromConfig(engineConfig));
        Optional<TableConfig.Format> defaultFormat = format.map(f -> new TableConfig.Format.DefaultFormat(f));

        return format1.isPresent() ? format1 : defaultFormat;
      }
    };
  }

  private ConnectorFactory createJdbcConnectorFactory(PackageJson.EngineConfig engineConfig1) {
    return new ConnectorFactory() {
      @Override
      public TableConfig createSourceAndSink(IConnectorFactoryContext context) {
        Map<String, Object> map = context.getMap();

        String tableName = (String)map.get("table-name");
        TableConfigBuilderImpl builder = TableConfigImpl.builder(tableName);
        builder.setType(ExternalDataType.sink);

        EngineConfigImpl engineConfig = (EngineConfigImpl) engineConfig1;
        builder.copyConnectorConfig(engineConfig);
        builder.getConnectorConfig().setProperty("table-name", tableName);
        builder.getConnectorConfig().setProperty("connector", "jdbc-sqrl");

        return builder.build();
      }

      @Override
      public Optional<TableConfig.Format> getFormat() {
        return Optional.empty();
      }
    };
  }
  //todo source

//  @MinLength(min = 3)
//  String url;
//  @MinLength(min = 2)
//  String dialect;
//  @Default
//  String database = null;
//  @Default
//  String host = null;
//  @Default
//  Integer port = null;
//  @Default
//  String user = null;
//  @Default
//  String password = null;
//  @Default @MinLength(min = 3)
//  String driver = null;
//
//  @Override
//  public JdbcDialect getDialect() {
//    return JdbcDialect.find(dialect).orElseThrow();
//  }
//
//  public static final Pattern JDBC_URL_REGEX = Pattern.compile("^jdbc:(.*?):\\/\\/([^/:]*)(?::(\\d+))?\\/([^/:?]*)(.*)$");
//  public static final Pattern JDBC_DIALECT_REGEX = Pattern.compile("^jdbc:(.*?):(.*)$");
////
////  public static JdbcDataSystemConnector fromFlinkConnector(@NonNull SqrlConfig connectorConfig) {
//////    Preconditions.checkArgument(connectorConfig.asString(FlinkConnectorFactory.CONNECTOR_KEY).get().equals("jdbc"));
////    JdbcDataSystemConnectorBuilder builder = builder();
////    String url = connectorConfig.asString("url").get();
////
////    //todo use: Properties properties = Driver.parseURL(connector.getUrl(), null);
////    builder.url(url);
////    Matcher matcher = JDBC_URL_REGEX.matcher(url);
////    if (matcher.find()) {
////      String dialect = matcher.group(1);
////      connectorConfig.getErrorCollector().checkFatal(JdbcDialect.find(dialect).isPresent(), "Invalid database dialect: %s", dialect);
////      builder.dialect(dialect);
////      builder.host(matcher.group(2));
////      builder.port(Integer.parseInt(matcher.group(3)));
////      builder.database(matcher.group(4));
////    } else {
////      //Only extract the dialect
////      matcher = JDBC_DIALECT_REGEX.matcher(url);
////      if (matcher.find()) {
////        String dialect = matcher.group(1);
////        connectorConfig.getErrorCollector().checkFatal(JdbcDialect.find(dialect).isPresent(), "Invalid database dialect: %s", dialect);
////        builder.dialect(dialect);
////      } else {
////        throw connectorConfig.getErrorCollector().exception("Invalid database URL: %s", url);
////      }
////    }
////    connectorConfig.asString("username").getOptional().ifPresent(builder::user);
////    connectorConfig.asString("password").getOptional().ifPresent(builder::password);
////    connectorConfig.asString("driver").getOptional().ifPresent(builder::driver);
////    return builder.build();
////  }
//
////  public SqrlConfig toFlinkConnector() {
////    SqrlConfig connectorConfig = SqrlConfig.createCurrentVersion();
////    connectorConfig.setProperty(FlinkConnectorFactory.CONNECTOR_KEY, "jdbc-sqrl");
////    connectorConfig.setProperty("url", url);
////    Optional.ofNullable(driver).ifPresent(v->connectorConfig.setProperty("driver", v));
////    Optional.ofNullable(user).ifPresent(v->connectorConfig.setProperty("username", v));
////    Optional.ofNullable(password).ifPresent(v->connectorConfig.setProperty("password", v));
////    return connectorConfig;
////  }
//
//
}
