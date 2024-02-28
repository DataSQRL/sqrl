package com.datasqrl.metadata;

import com.datasqrl.config.SqrlConfig;
import lombok.NonNull;

public interface MetadataEngine {
  MetadataStoreProvider getMetadataStore(@NonNull SqrlConfig config);
}
