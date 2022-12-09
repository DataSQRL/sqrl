/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class Dependency {

  String name;
  String version;
  String variant;

  public Dependency normalize(String name) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.version));
    if (!Strings.isNullOrEmpty(this.name)) {
      name = this.name;
    }
    String variant = "";
    if (!Strings.isNullOrEmpty(this.variant)) {
      variant = this.variant;
    }
    return new Dependency(name, this.version, variant);
  }

}
