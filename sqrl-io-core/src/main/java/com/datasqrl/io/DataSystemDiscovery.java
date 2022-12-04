/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.name.Name;
import lombok.NonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public interface DataSystemDiscovery {

  /**
   * The name of the dataset produced by this data source if discoverable from the configuration
   *
   * @return name of dataset
   */
  @NonNull Optional<String> getDefaultName();

  boolean requiresFormat(ExternalDataType type);

  default Collection<TableConfig> discoverSources(@NonNull DataSystemConfig config,
      @NonNull ErrorCollector errors) {
    return Collections.EMPTY_LIST;
  }

  default Optional<TableConfig> discoverSink(@NonNull Name sinkName,
      @NonNull DataSystemConfig config,
      @NonNull ErrorCollector errors) {
    return Optional.empty();
  }

}
