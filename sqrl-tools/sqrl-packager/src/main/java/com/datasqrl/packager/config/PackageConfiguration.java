/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.config;

import com.datasqrl.config.Constraints.Default;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PackageConfiguration {

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
  String readme = "";
  @Default
  String description = "";
  @Default
  List<String> keywords = List.of();

  public static PackageConfiguration fromRootConfig(@NonNull SqrlConfig rootConfig) {
    return rootConfig.getSubConfig(PACKAGE_KEY).allAs(PackageConfiguration.class).get();
  }

  public void checkInitialized() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getName()) &&
        !Strings.isNullOrEmpty(getVersion()) && !Strings.isNullOrEmpty(getVariant()) &&
        getLatest()!=null && getKeywords()!=null, "Package configuration has not been initialized.");
  }

  @JsonIgnore
  public Dependency asDependency() {
    checkInitialized();
    return new Dependency(getName(), getVersion(), getVariant());
  }


}
