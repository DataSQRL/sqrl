package com.datasqrl.config;

import java.util.List;
import java.util.Map;

public interface PackageConfiguration {

  String getName();

  String getVersion();

  String getVariant();

  Boolean getLatest();

  String getType();

  String getLicense();

  String getRepository();

  String getHomepage();

  String getDescription();

  String getReadme();

  String getDocumentation();

  List<String> getTopics();

  List<String> getSources();

  void checkInitialized();

  Dependency asDependency();

  Map<String, Object> toMap();
}
