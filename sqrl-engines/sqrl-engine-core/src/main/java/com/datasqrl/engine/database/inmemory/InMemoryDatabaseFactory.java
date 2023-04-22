package com.datasqrl.engine.database.inmemory;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.DatabaseEngineFactory;
import com.datasqrl.engine.database.inmemory.InMemoryDatabaseConfiguration.StoreProvider;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.metadata.MetadataStore;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class InMemoryDatabaseFactory implements DatabaseEngineFactory {

  public static final String ENGINE_NAME = "hashmap";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public DatabaseEngine initialize(@NonNull SqrlConfig config) {
    return new InMemoryDatabase();
  }

  @Override
  public MetadataStoreProvider getMetadataStore(@NonNull SqrlConfig config) {
    return new InMemoryDatabaseConfiguration.StoreProvider();
  }

  public static class StoreProvider implements MetadataStoreProvider {

    @Override
    public MetadataStore openStore() {
      return new InMemoryMetadataStore();
    }
  }

}
