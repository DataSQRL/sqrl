/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.print;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.*;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.canonicalizer.Name;
import lombok.NonNull;

import java.util.Optional;

public abstract class PrintDataSystem {

  public static final String PREFIX_KEY = "prefix";

  public static class Connector implements DataSystemConnector {

    @Override
    public boolean hasSourceTimestamp() {
      return false;
    }
  }

  public static class Discovery extends DataSystemDiscovery.Base {

    public Discovery(TableConfig baseTable) {
      super(baseTable);
    }

    @Override
    public boolean requiresFormat(ExternalDataType type) {
      return true;
    }

    @Override
    public Optional<TableConfig> discoverSink(@NonNull Name sinkName,
        @NonNull ErrorCollector errors) {
      return Optional.of(copyGeneric(sinkName, ExternalDataType.sink).build());
    }

  }

}
