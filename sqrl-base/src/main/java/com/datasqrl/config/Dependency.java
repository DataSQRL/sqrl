package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import java.util.Optional;

public interface Dependency {

  String getName();

  void setName(String name);

  Optional<String> getVersion();

  void setVersion(String version);

  String getVariant();

  void setVariant(String variant);

  Dependency normalize(String defaultName, ErrorCollector errors);
}
