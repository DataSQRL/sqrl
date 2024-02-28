package com.datasqrl.metadata;

import com.datasqrl.config.SqrlConfig;
import lombok.NonNull;

public class InMemoryMetadataStore implements MetadataEngine {

  @Override
  public MetadataStoreProvider getMetadataStore(@NonNull SqrlConfig config) {
    return new StoreProvider();
  }

  public static class StoreProvider implements MetadataStoreProvider {

    @Override
    public MetadataStore openStore() {
      return new com.datasqrl.engine.database.inmemory.InMemoryMetadataStore();
    }
  }

}
