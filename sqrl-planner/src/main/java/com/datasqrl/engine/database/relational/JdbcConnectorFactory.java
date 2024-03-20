package com.datasqrl.engine.database.relational;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.io.tables.ConnectorFactory;
import lombok.NonNull;

public interface JdbcConnectorFactory extends ConnectorFactory {

  ConnectorConfig fromBaseConfig(@NonNull SqrlConfig connectorConfig, @NonNull String topic);

}
