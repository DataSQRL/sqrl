/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner.dag.plan;

import com.datasqrl.planner.parser.SqrlTableFunctionStatement;
import com.datasqrl.planner.tables.MetadataExtractor;
import com.datasqrl.server.MetadataType;
import com.datasqrl.server.ResolvedMetadata;
import java.util.Optional;

public final class MutationMetadataExtractor implements MetadataExtractor {

  public static final String UUID_METADATA = "uuid";
  public static final String TIMESTAMP_METADATA = "timestamp";

  /**
   * Resolves the metadata alias of a mutation column, or returns empty for an unsupported alias.
   */
  @Override
  public Optional<ResolvedMetadata> extract(String metadataAlias, boolean nullable) {
    if (metadataAlias.equalsIgnoreCase(UUID_METADATA)) {
      return Optional.of(new ResolvedMetadata(MetadataType.UUID, !nullable));
    }

    if (metadataAlias.equalsIgnoreCase(TIMESTAMP_METADATA)) {
      return Optional.of(new ResolvedMetadata(MetadataType.TIMESTAMP, !nullable));
    }

    return SqrlTableFunctionStatement.parseMetadata(metadataAlias, !nullable);
  }

  @Override
  public boolean removeMetadata(String metadataAlias) {
    // Remove for all but timestamp
    return !metadataAlias.equalsIgnoreCase(TIMESTAMP_METADATA);
  }

  @Override
  public String supportedAliases() {
    return "'uuid', 'timestamp' or 'auth.<claim>'";
  }
}
