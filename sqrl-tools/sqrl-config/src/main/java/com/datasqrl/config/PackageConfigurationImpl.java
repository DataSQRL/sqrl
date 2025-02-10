/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.config.Constraints.Default;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PackageConfigurationImpl implements PackageConfiguration {

  public static final String PACKAGE_KEY = "package";

  public static final String DEFAULT_VARIANT = "default";

  String name;
  String version;
  @Default
  String variant = DEFAULT_VARIANT;
  @Default
  Boolean latest = true;
  @Default
  String type = null;
  @Default
  String license = "";
  @Default
  String repository = "";
  @Default
  String homepage = "";
  @Default
  String documentation = "";
  @Default
  @Setter
  String readme = null;
  @Default
  String description = "";
  @Default
  List<String> topics = List.of();

  public void checkInitialized() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getName()) &&
        !Strings.isNullOrEmpty(getVersion()) && !Strings.isNullOrEmpty(getVariant()) &&
        getLatest() != null && this.getTopics() != null, "Package configuration has not been initialized.");
  }

  @JsonIgnore
  public DependencyImpl asDependency() {
    checkInitialized();
    return new DependencyImpl(getName());
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("name", getName());
    map.put("version", getVersion());
    map.put("variant", getVariant());
    map.put("latest", getLatest());
    map.put("type", getType());
    map.put("license", getLicense());
    map.put("repository", getRepository());
    map.put("homepage", getHomepage());
    map.put("documentation", getDocumentation());
    map.put("readme", getReadme());
    map.put("description", getDescription());
    map.put("topics", this.getTopics());
    return map;
  }
}
