package com.datasqrl.engine.kafka;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.io.tables.ConnectorFactory;
import lombok.NonNull;

public interface KafkaConnectorFactory extends ConnectorFactory {

  String getEventTime();

  ConnectorConfig fromBaseConfig(@NonNull SqrlConfig connectorConfig, @NonNull String topic);

  String getTopic(@NonNull SqrlConfig connectorConfig);


}
