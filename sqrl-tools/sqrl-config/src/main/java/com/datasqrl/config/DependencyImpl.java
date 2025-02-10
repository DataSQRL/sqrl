/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode
@Setter
@AllArgsConstructor
public class DependencyImpl implements Dependency {
//
  @Constraints.Default
  String name = null;

  public DependencyImpl() {
  }
  
  public DependencyImpl(SqrlConfig sqrlConfig) {
    name = sqrlConfig.asString("name").getOptional()
        .orElse(null);
  }

  @Override
  public String toString() {
    return getName();
  }

  /**
   * Normalizes a dependency and uses the dependency package name as the name unless it is explicitly specified.
   * @param defaultName
   * @return
   */
  @Override
  public Dependency normalize(String defaultName, ErrorCollector errors) {
    errors.checkFatal(!Strings.isNullOrEmpty(defaultName),"Invalid dependency name: %s", defaultName);
    String name;
    if (Strings.isNullOrEmpty(this.getName())) {
      name = defaultName;
    } else {
      name = this.getName();
    }
    return new DependencyImpl(name);
  }
}
