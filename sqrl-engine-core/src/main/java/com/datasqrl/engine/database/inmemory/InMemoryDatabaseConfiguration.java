/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.inmemory;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.metadata.MetadataStore;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.DatabaseEngineConfiguration;
import lombok.NonNull;

public class InMemoryDatabaseConfiguration implements DatabaseEngineConfiguration {

  public static final String ENGINE_NAME = "hashmap";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public DatabaseEngine initialize(@NonNull ErrorCollector errors) {
    return new InMemoryDatabase();
  }

  @Override
  public MetadataStoreProvider getMetadataStore() {
    return new StoreProvider();
  }

  public static class StoreProvider implements MetadataStoreProvider {

    @Override
    public MetadataStore openStore() {
      return new InMemoryMetadataStore();
    }
  }

}
