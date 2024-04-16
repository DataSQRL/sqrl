package com.datasqrl.config;

import java.util.List;

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

  List<String> getKeywords();

  void checkInitialized();

  Dependency asDependency();
}
