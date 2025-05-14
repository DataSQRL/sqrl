package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;

public interface Dependency {

  String getName();

  void setName(String name);

  Dependency normalize(String defaultName, ErrorCollector errors);

}
