package com.datasqrl.config;

import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import java.util.Map;
import lombok.Value;

@Value
public class ConnectorFactoryContext implements IConnectorFactoryContext {
  Map<String, Object> map;
}
