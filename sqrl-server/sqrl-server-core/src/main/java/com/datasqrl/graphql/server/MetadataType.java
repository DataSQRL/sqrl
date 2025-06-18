package com.datasqrl.graphql.server;

import lombok.NonNull;

public enum MetadataType {
  AUTH;

  public record ResolvedMetadata(@NonNull MetadataType metadataType, String name) {}
}
