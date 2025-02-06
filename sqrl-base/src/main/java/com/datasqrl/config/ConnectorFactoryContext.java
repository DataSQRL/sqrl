package com.datasqrl.config;

import java.util.Map;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorFactory.IConnectorFactoryContext;

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
