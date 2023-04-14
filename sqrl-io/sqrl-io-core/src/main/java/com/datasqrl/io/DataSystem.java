/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

@AllArgsConstructor
@Getter
public class DataSystem implements Serializable {

  Name name;
  DataSystemDiscovery datasource;
  DataSystemConfig config;

  public NameCanonicalizer getCanonicalizer() {
    return config.getNameCanonicalizer();
  }

  public Collection<TableConfig> discoverTables(ErrorCollector errors) {
    return datasource.discoverSources(config, errors);
  }

  public Optional<TableConfig> discoverSink(@NonNull Name sinkName,
      @NonNull ErrorCollector errors) {
    return datasource.discoverSink(sinkName, config, errors);
  }

}
