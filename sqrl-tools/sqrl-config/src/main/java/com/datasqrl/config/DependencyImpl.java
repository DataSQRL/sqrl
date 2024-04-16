/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import static com.datasqrl.config.DependenciesConfigImpl.PKG_NAME_KEY;
import static com.datasqrl.config.DependenciesConfigImpl.VARIANT_KEY;
import static com.datasqrl.config.SqrlConfig.VERSION_KEY;

import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Strings;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@NoArgsConstructor
public class DependencyImpl implements Dependency {
//
//  @Constraints.Default
//  String name = null;
//  String version;
//  @Constraints.Default
//  String variant = PackageConfiguration.DEFAULT_VARIANT;

  SqrlConfig sqrlConfig;

  public DependencyImpl(String name, String version, String variant) {
    sqrlConfig = SqrlConfig.createCurrentVersion();
    setName(name);
    setVersion(name);
    setVariant(name);
  }

  @Override
  public String toString() {
    return getName() + "@" + getVersion().orElse("1") + "/" + getVariant();
  }

  @Override
  public String getName() {
    return sqrlConfig.asString(PKG_NAME_KEY).getOptional()
        .orElse(null);
  }

  @Override
  public void setName(String name) {
    sqrlConfig.setProperty(PKG_NAME_KEY, name);
  }

  @Override
  public Optional<String> getVersion() {
    return sqrlConfig.asString(VERSION_KEY).getOptional();

  }
  @Override
  public void setVersion(String version) {
    sqrlConfig.setProperty(VERSION_KEY, version);
  }

  @Override
  public String getVariant() {
    return sqrlConfig.asString(VARIANT_KEY).getOptional()
        .orElse(PackageConfigurationImpl.DEFAULT_VARIANT);
  }

  @Override
  public void setVariant(String variant) {
    sqrlConfig.setProperty(VARIANT_KEY, variant);
  }
  /**
   * Normalizes a dependency and uses the dependency package name as the name unless it is explicitly specified.
   * @param defaultName
   * @return
   */
  @Override
  public Dependency normalize(String defaultName, ErrorCollector errors) {
    errors.checkFatal(!Strings.isNullOrEmpty(defaultName),"Invalid dependency name: %s", defaultName);
    errors.checkFatal(this.getVersion().isPresent() && !Strings.isNullOrEmpty(this.getVersion().get()),"Need to specify a version for dependency [%s]", defaultName);
    String name;
    if (Strings.isNullOrEmpty(this.getName())) {
      name = defaultName;
    } else {
      name = this.getName();
    }
    String variant;
    if (Strings.isNullOrEmpty(this.getVariant())) {
      variant = PackageConfigurationImpl.DEFAULT_VARIANT;
    } else {
      variant = getVariant();
    }
    return new DependencyImpl(name, getVersion().orElseThrow(), variant);
  }
}
