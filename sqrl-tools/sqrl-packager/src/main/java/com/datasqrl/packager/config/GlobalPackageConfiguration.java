/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.config;

import com.datasqrl.config.GlobalConfiguration;
import com.datasqrl.config.ScriptConfiguration;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import javax.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class GlobalPackageConfiguration implements GlobalConfiguration {

  public static final String DEPENDENCIES_NAME = "dependencies";
  public static final String PACKAGE_NAME = "package";

  @JsonProperty(PACKAGE_NAME)
  @Builder.Default
  @Valid
  PackageConfiguration pkg = new PackageConfiguration();

  @JsonProperty(DEPENDENCIES_NAME)
  @NonNull
  @Builder.Default
  @Valid
  @Setter
  LinkedHashMap<String, Dependency> dependencies = new LinkedHashMap<>();

  @Setter
  @JsonProperty(ScriptConfiguration.PROPERTY)
  ScriptConfiguration script;

  public static GlobalPackageConfiguration readFrom(Path path) throws IOException {
    ObjectMapper mapper = SqrlObjectMapper.INSTANCE;
    return mapper.readValue(path.toFile(), GlobalPackageConfiguration.class);
  }

}
