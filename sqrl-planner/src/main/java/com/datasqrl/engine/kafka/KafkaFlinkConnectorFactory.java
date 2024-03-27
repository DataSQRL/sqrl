package com.datasqrl.engine.kafka;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.io.tables.FlinkConnectorFactory;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class KafkaFlinkConnectorFactory extends FlinkConnectorFactory implements KafkaConnectorFactory {

  public static final KafkaConnectorFactory INSTANCE = new KafkaFlinkConnectorFactory();
  public static final String TOPIC_KEY = "topic";

  String eventTime = "timestamp";

  private KafkaFlinkConnectorFactory() {
  }

  @Override
  public ConnectorConfig fromBaseConfig(@NonNull SqrlConfig connectorConfig, @NonNull String topic) {
    SqrlConfig config = SqrlConfig.create(connectorConfig);
    config.copy(connectorConfig);
    config.setProperty(TOPIC_KEY, topic);
    return new ConnectorConfig(config, this);
  }

  @Override
  public String getTopic(@NonNull SqrlConfig connectorConfig) {
    return connectorConfig.asString(TOPIC_KEY).get();
  }


}
