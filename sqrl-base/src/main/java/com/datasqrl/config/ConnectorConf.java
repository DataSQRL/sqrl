package com.datasqrl.config;

import java.util.Map;
import java.util.function.Predicate;

public interface ConnectorConf {

  Map<String, Object> toMap();

  Map<String, Object> toMapWithSubstitution(Map<String, String> variables);

  void validate(String key, Predicate<String> validator, String msg);
}
