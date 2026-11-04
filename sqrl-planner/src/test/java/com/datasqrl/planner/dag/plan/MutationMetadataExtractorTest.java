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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.server.MetadataType;
import com.datasqrl.server.ResolvedMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MutationMetadataExtractorTest {

  private final MutationMetadataExtractor extractor = new MutationMetadataExtractor();

  @Test
  void givenUuidAlias_whenExtract_thenReturnsUuidMetadata() {
    assertThat(extractor.extract("uuid", false))
        .contains(new ResolvedMetadata(MetadataType.UUID, true));
  }

  @Test
  void givenTimestampAlias_whenExtract_thenReturnsTimestampMetadata() {
    assertThat(extractor.extract("timestamp", true))
        .contains(new ResolvedMetadata(MetadataType.TIMESTAMP, false));
  }

  @Test
  void givenAuthClaimAlias_whenExtract_thenReturnsAuthMetadataWithClaimName() {
    assertThat(extractor.extract("auth.sub", false))
        .contains(new ResolvedMetadata(MetadataType.AUTH, "sub", true));
  }

  @ParameterizedTest
  @ValueSource(strings = {"auht.sub", "offset", "uuid.id", "timestamp.ts", "auth", "auth."})
  void givenUnsupportedAlias_whenExtract_thenReturnsEmpty(String metadataAlias) {
    assertThat(extractor.extract(metadataAlias, false)).isEmpty();
  }
}
