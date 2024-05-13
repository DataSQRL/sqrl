package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class ConnectorFactoryContext implements IConnectorFactoryContext {
  Name name;
  Map<String, Object> map;

  public ConnectorFactoryContext(String name, Map<String, Object> map) {
    this(Name.system(name), map);
  }

}
