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
package com.datasqrl.discovery.preprocessor;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.properties.SystemProperties;

@ExtendWith(SystemStubsExtension.class)
class DiscoverySchemaCacheTest {

  private static final String RECORDS =
      """
      {"id": 1, "name": "a", "time": "2024-01-01T10:00:00Z", "tags": ["x", "y"], "nested": {"k": 1}, "mixed": {"v": 1}}
      {"id": 2, "name": null, "time": "2024-01-02T10:00:00Z", "tags": [], "nested": {"k": 2, "extra": "e"}, "mixed": "scalar"}
      {"id": "3", "name": "c", "time": "2024-01-03T10:00:00Z", "tags": ["z"], "nested": null, "mixed": null, "late": 1.5}
      """;

  private static final String STRING = "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"";

  @SystemStub private SystemProperties properties;

  @TempDir Path tempDir;

  private Path dataFile;
  private Path cacheDir;
  private final JsonlDiscoveryTableSchemaFactory factory = new JsonlDiscoveryTableSchemaFactory();

  @BeforeEach
  void setUp() throws IOException {
    dataFile = tempDir.resolve("records.jsonl");
    Files.writeString(dataFile, RECORDS);
    cacheDir = tempDir.resolve("cache");
  }

  @Test
  void disabledWithoutConfiguration() {
    discover();
    assertThat(tempDir.resolve("cache")).doesNotExist();
  }

  @Test
  void hitMatchesUncachedDiscovery() throws IOException {
    var uncached = discover();
    properties.set(DiscoverySchemaCache.CACHE_DIR_PROPERTY, cacheDir.toString());

    var miss = discover();
    var entry = singleEntry();
    var written = Files.readString(entry);
    var hit = discover();

    assertThat(miss).isEqualTo(uncached);
    assertThat(hit).isEqualTo(uncached);
    assertThat(Files.readString(entry)).isEqualTo(written);
    assertThat(uncached)
        .isEqualTo(
            "RecordType(BIGINT NOT NULL id, "
                + STRING
                + " name, TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL time, "
                + STRING
                + " NOT NULL ARRAY tags, RecordType(BIGINT k, "
                + STRING
                + " extra) nested, RecordType(BIGINT v, "
                + STRING
                + " _value) mixed, DOUBLE late) NOT NULL");
  }

  @Test
  void hitIsServedFromCache() throws IOException {
    properties.set(DiscoverySchemaCache.CACHE_DIR_PROPERTY, cacheDir.toString());
    discover();
    var entry = singleEntry();
    Files.writeString(
        entry,
        """
        {"fields":[{"name":{"canonical":"only","display":"only"},"types":[{"variant":{"canonical":" #singleton","display":" #singleton"},"basicType":"STRING","fields":null,"arrayDepth":0,"constraints":[{"name":"not_null","parameters":null}]}]}]}
        """);

    assertThat(discover()).isEqualTo("RecordType(" + STRING + " NOT NULL only) NOT NULL");
  }

  @Test
  void corruptEntryIsReplaced() throws IOException {
    var uncached = discover();
    properties.set(DiscoverySchemaCache.CACHE_DIR_PROPERTY, cacheDir.toString());
    discover();
    var entry = singleEntry();
    var written = Files.readString(entry);
    Files.writeString(entry, "{not json");

    assertThat(discover()).isEqualTo(uncached);
    assertThat(Files.readString(entry)).isEqualTo(written);
  }

  @Test
  void unknownTypeInEntryIsReplaced() throws IOException {
    var uncached = discover();
    properties.set(DiscoverySchemaCache.CACHE_DIR_PROPERTY, cacheDir.toString());
    discover();
    var entry = singleEntry();
    var written = Files.readString(entry);
    Files.writeString(entry, written.replace("\"STRING\"", "\"NO_SUCH_TYPE\""));

    assertThat(discover()).isEqualTo(uncached);
    assertThat(Files.readString(entry)).isEqualTo(written);
  }

  @Test
  void keyChangesWithContentAndSettings() throws IOException {
    properties.set(DiscoverySchemaCache.CACHE_DIR_PROPERTY, cacheDir.toString());
    discover();
    var full = singleEntry();

    properties.set(DiscoverySchemaCache.MAX_RECORDS_PROPERTY, "1");
    var sampled = discover();
    assertThat(sampled)
        .doesNotContain("late")
        .contains("RecordType(BIGINT NOT NULL k) NOT NULL nested");
    assertThat(entries()).hasSize(2).contains(full);

    Files.writeString(dataFile, RECORDS + "{\"id\": 4}\n");
    discover();
    assertThat(entries()).hasSize(3);
  }

  private String discover() {
    return factory.convert(dataFile, Map.of(), ErrorCollector.root()).type().getFullTypeString();
  }

  private Path singleEntry() throws IOException {
    var entries = entries();
    assertThat(entries).hasSize(1);
    return entries.get(0);
  }

  private List<Path> entries() throws IOException {
    try (Stream<Path> files = Files.list(cacheDir)) {
      return files.filter(path -> path.toString().endsWith(".json")).toList();
    }
  }
}
