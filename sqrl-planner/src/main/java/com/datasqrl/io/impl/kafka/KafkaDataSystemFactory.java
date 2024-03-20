package com.datasqrl.io.impl.kafka;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigUtil;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.DataSystemDiscoveryFactory;
import com.datasqrl.io.DataSystemImplementationFactory;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FormatFactoryOld;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.tables.TableConfig;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Properties;
import java.util.Set;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaDataSystemFactory implements DataSystemImplementationFactory {

  public static final String SYSTEM_NAME = "kafka";

  public static final String TOPIC_PREFIX_KEY = "prefix";

  public static final Set<String> ALL_KEYS = Set.of(
      SYSTEM_NAME_KEY,
      TOPIC_PREFIX_KEY);

  @Override
  public String getSystemName() {
    return SYSTEM_NAME;
  }

  @AutoService(DataSystemConnectorFactory.class)
  public static class Connector extends KafkaDataSystemFactory
      implements DataSystemConnectorFactory {

    @Override
    public DataSystemConnectorSettings getSettings(@NonNull SqrlConfig connectorConfig) {
      return DataSystemConnectorSettings.builder().hasSourceTimestamp(true).build();
    }

  }

  @AutoService(DataSystemDiscoveryFactory.class)
  public static class Discovery extends KafkaDataSystemFactory implements
      DataSystemDiscoveryFactory {

    @Override
    public DataSystemDiscovery initialize(@NonNull TableConfig tableConfig) {
      SqrlConfig connectorConfig = tableConfig.getConnectorConfigOld();
      return new KafkaDataSystemDiscovery(tableConfig,
          connectorConfig.asString(TOPIC_PREFIX_KEY).withDefault("").get(),
          getKafkaProperties(connectorConfig));
    }


  }

  public Properties getKafkaProperties(@NonNull SqrlConfig connectorConfig) {
    return SqrlConfigUtil.toProperties(connectorConfig, Set.of(SYSTEM_NAME_KEY, TOPIC_PREFIX_KEY));
  }

  public static TableConfig.Builder getKafkaConfig(@NonNull String name, @NonNull String servers, String prefix) {
    BaseTableConfig baseTable = BaseTableConfig.builder()
        .type(ExternalDataType.source_and_sink.name())
        .build();
    return getKafkaConfig(name, baseTable, servers, prefix);
  }

  public static SqrlConfig getKafkaEngineConfig(@NonNull String name, @NonNull String server,
                                                @NonNull String format, @NonNull String schema) {
    BaseTableConfig baseTable = BaseTableConfig.builder()
            .schema(schema)
            .type(ExternalDataType.source_and_sink.name())
            .build();
    TableConfig.Builder tblBuilder = KafkaDataSystemFactory.getKafkaConfig(name,
            baseTable, server, null);
    tblBuilder.getFormatConfig().setProperty(FormatFactoryOld.FORMAT_NAME_KEY, format);
    return tblBuilder.getConfig();
  }

  public static SqrlConfig getKafkaEngineConfigWithTopic(@NonNull String name, @NonNull String server,
                                                         @NonNull String topic,
                                                         @NonNull String format, @NonNull String schema) {
    SqrlConfig config = getKafkaEngineConfig(name, server, format, schema);
    config.setProperty(BaseTableConfig.IDENTIFIER_KEY, topic);
    return config;
  }

  public static TableConfig.Builder getKafkaConfig(@NonNull String name, @NonNull BaseTableConfig baseTable,
                                                   @NonNull String servers, String prefix) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(servers));
    TableConfig.Builder tblBuilder = TableConfig.builder(Name.system(name));//.base(baseTable);
    SqrlConfig connectorConfig = tblBuilder.getConnectorConfig();
    connectorConfig.setProperty(SYSTEM_NAME_KEY, SYSTEM_NAME);
    if (!Strings.isNullOrEmpty(prefix)) connectorConfig.setProperty(TOPIC_PREFIX_KEY, prefix);
    connectorConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    return tblBuilder;
  }

}
