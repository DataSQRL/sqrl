package com.datasqrl.planner.tables;

import com.datasqrl.graphql.server.ResolvedMetadata;
import java.util.Optional;

public interface MetadataExtractor {

  ResolvedMetadata convert(String metadataAlias, boolean isNullable);

  boolean removeMetadata(String metadataAlias);
}
