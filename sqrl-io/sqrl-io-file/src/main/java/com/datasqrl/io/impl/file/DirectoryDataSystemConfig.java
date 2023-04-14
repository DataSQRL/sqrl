/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.file;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.DataSystemDiscoveryConfig;
import com.datasqrl.io.impl.file.DirectoryDataSystem.DirectoryConnector;
import com.datasqrl.util.constraints.OptionalMinString;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public abstract class DirectoryDataSystemConfig {

  public static final String SYSTEM_TYPE = "dir";
  public static final String DEFAULT_FILENAME_PATTERN = "^([^\\.]+?)(?:_part.*)?$";

  @Builder.Default
  String directoryURI = "";

  @Builder.Default
  List<String> fileURIs = List.of();

  @Builder.Default
  @NonNull @NotNull
  String filenamePattern = DEFAULT_FILENAME_PATTERN;

  @JsonIgnore
  FilePathConfig filePathConfig;

  protected boolean rootInitialize(@NonNull ErrorCollector errors) {
    filePathConfig = FilePathConfig.of(directoryURI, fileURIs, errors);
    return filePathConfig != null;
  }

  public String getSystemType() {
    return SYSTEM_TYPE;
  }

  @JsonIgnore
  protected Pattern compilerFilenamePattern() {
    String regex = filenamePattern;
    if (!regex.startsWith("^")) {
      regex = "^" + regex;
    }
    if (!regex.endsWith("$")) {
      regex += "$";
    }
    return Pattern.compile(regex);
  }

  @SuperBuilder
  @NoArgsConstructor
  @AutoService(DataSystemConnectorConfig.class)
  public static class Connector extends DirectoryDataSystemConfig implements
      DataSystemConnectorConfig {

    private Connector(Discovery discovery) {
      super(discovery.directoryURI, discovery.fileURIs, discovery.filenamePattern, null);
    }

    @Override
    public DataSystemConnector initialize(@NonNull ErrorCollector errors) {
      if (rootInitialize(errors)) {
        return new DirectoryConnector(filePathConfig, compilerFilenamePattern());
      } else {
        return null;
      }
    }
  }

  @SuperBuilder
  @NoArgsConstructor
  @AutoService(DataSystemDiscoveryConfig.class)
  public static class Discovery extends DirectoryDataSystemConfig implements
      DataSystemDiscoveryConfig {

    @Override
    public DataSystemDiscovery initialize(@NonNull ErrorCollector errors) {
      if (rootInitialize(errors)) {
        return new DirectoryDataSystem.Discovery(getFilePathConfig(), compilerFilenamePattern(),
            new Connector(this));
      } else {
        return null;
      }
    }

  }

  public static DataSystemDiscoveryConfig ofDirectory(Path path) {
    return Discovery.builder().directoryURI(path.toUri().getPath()).build();
  }

}
