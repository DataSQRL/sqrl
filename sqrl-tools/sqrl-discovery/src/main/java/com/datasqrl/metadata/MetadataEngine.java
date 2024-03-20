package com.datasqrl.metadata;

import com.datasqrl.engine.database.DatabaseEngine;
import lombok.NonNull;

public interface MetadataEngine {
  MetadataStoreProvider getMetadataStore(@NonNull DatabaseEngine databaseEngine);
}
