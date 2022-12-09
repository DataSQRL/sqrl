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
import lombok.ToString;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@ToString
public class Dependency {

  String name;
  String version;
  String variant;

  public Dependency normalize(String defaultName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.version));
    if (Strings.isNullOrEmpty(this.name)) {
      this.name = defaultName;
    }
    if (Strings.isNullOrEmpty(this.variant)) {
      this.variant = "";
    }
    return this;
  }

}
