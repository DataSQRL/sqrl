/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.print;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.*;
import com.datasqrl.io.formats.FileFormat;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.canonicalizer.Name;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.auto.service.AutoService;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.Optional;

@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public abstract class PrintDataSystem {

  public static final String SYSTEM_TYPE = "print";

  String prefix = "";

  public String getSystemType() {
    return SYSTEM_TYPE;
  }

  private static final PrintDataSystem.Discovery DEFAULT_DISCOVERY = new Discovery();
  public static final DataSystemConfig DEFAULT_DISCOVERY_CONFIG = DataSystemConfig.builder()
      .name(SYSTEM_TYPE)
      .datadiscovery(DEFAULT_DISCOVERY)
      .type(ExternalDataType.sink)
      .format(FileFormat.JSON.getImplementation().getDefaultConfiguration())
      .build();

  @SuperBuilder
  @NoArgsConstructor
  @AutoService(DataSystemConnectorConfig.class)
  public static class Connector extends PrintDataSystem implements DataSystemConnectorConfig,
      DataSystemConnector {

    public Connector(String prefix) {
      super(prefix);
    }

    @Override
    public DataSystemConnector initialize(@NonNull ErrorCollector errors) {
      return this;
    }

    @Override
    @JsonIgnore
    public boolean hasSourceTimestamp() {
      return false;
    }
  }

  @SuperBuilder
  @NoArgsConstructor
  @AutoService(DataSystemDiscoveryConfig.class)
  public static class Discovery extends PrintDataSystem implements DataSystemDiscoveryConfig,
      DataSystemDiscovery {

    @Override
    @JsonIgnore
    public @NonNull Optional<String> getDefaultName() {
      return Optional.of(SYSTEM_TYPE);
    }

    @Override
    public boolean requiresFormat(ExternalDataType type) {
      return true;
    }

    @Override
    public Optional<TableConfig> discoverSink(@NonNull Name sinkName,
        @NonNull DataSystemConfig config, @NonNull ErrorCollector errors) {
      TableConfig.TableConfigBuilder tblBuilder = TableConfig.copy(config);
      tblBuilder.type(ExternalDataType.sink);
      tblBuilder.identifier(sinkName.getCanonical());
      tblBuilder.name(sinkName.getDisplay());
      tblBuilder.connector(new Connector(prefix));
      return Optional.of(tblBuilder.build());
    }

    @Override
    public DataSystemDiscovery initialize(@NonNull ErrorCollector errors) {
      return this;
    }
  }

}
