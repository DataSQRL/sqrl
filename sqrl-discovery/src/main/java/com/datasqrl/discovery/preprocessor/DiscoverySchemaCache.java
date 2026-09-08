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

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.SchemaAdjustmentSettings;
import com.datasqrl.util.ProjectConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DiscoverySchemaCache {

  public static final String CACHE_DIR_PROPERTY = "sqrl.discovery.cache.dir";
  public static final String CACHE_DIR_ENV = "SQRL_DISCOVERY_CACHE_DIR";
  public static final String MAX_RECORDS_PROPERTY = "sqrl.discovery.max-records";
  public static final String MAX_RECORDS_ENV = "SQRL_DISCOVERY_MAX_RECORDS";

  private static final String FORMAT_VERSION = "1";
  private static final String ENTRY_SUFFIX = ".json";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Optional<Path> directory;
  @Getter private final long maxRecords;

  public DiscoverySchemaCache(Optional<Path> directory, long maxRecords) {
    Preconditions.checkArgument(maxRecords > 0, "Max records must be positive: %s", maxRecords);
    this.directory = directory;
    this.maxRecords = maxRecords;
  }

  public static DiscoverySchemaCache fromEnvironment() {
    var directory = setting(CACHE_DIR_PROPERTY, CACHE_DIR_ENV).map(Path::of);
    var maxRecords =
        setting(MAX_RECORDS_PROPERTY, MAX_RECORDS_ENV).map(Long::parseLong).orElse(Long.MAX_VALUE);
    return new DiscoverySchemaCache(directory, maxRecords);
  }

  private static Optional<String> setting(String property, String env) {
    return Optional.ofNullable(System.getProperty(property))
        .or(() -> Optional.ofNullable(System.getenv(env)))
        .filter(value -> !value.isBlank());
  }

  public Optional<String> key(Path dataFile, String format, SchemaAdjustmentSettings settings) {
    if (directory.isEmpty()) {
      return Optional.empty();
    }
    var digest = sha256();
    try (InputStream in = Files.newInputStream(dataFile)) {
      var buffer = new byte[64 * 1024];
      int read;
      while ((read = in.read(buffer)) >= 0) {
        digest.update(buffer, 0, read);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    var metadata =
        String.join(
            "\n",
            FORMAT_VERSION,
            ProjectConstants.SQRL_VERSION,
            format,
            dataFile.getFileName().toString(),
            Long.toString(maxRecords),
            fingerprint(settings));
    digest.update(metadata.getBytes(StandardCharsets.UTF_8));
    return Optional.of(HexFormat.of().formatHex(digest.digest()));
  }

  public Optional<FlexibleTableSchema> lookup(String key, Name tableName) {
    var entry = entry(key);
    if (!Files.isRegularFile(entry)) {
      return Optional.empty();
    }
    try {
      var json = MAPPER.readValue(entry.toFile(), FlexibleSchemaJson.class);
      var builder = new FlexibleTableSchema.Builder();
      builder.setName(tableName);
      builder.setPartialSchema(false);
      builder.setFields(json.toRelation());
      log.debug("Loaded discovered schema for {} from {}", tableName, entry);
      return Optional.of(builder.build());
    } catch (IOException | RuntimeException e) {
      log.warn("Ignoring unreadable discovery cache entry {}: {}", entry, e.toString());
      return Optional.empty();
    }
  }

  public void store(String key, FlexibleTableSchema schema) {
    var entry = entry(key);
    try {
      Files.createDirectories(entry.getParent());
      var temp = Files.createTempFile(entry.getParent(), key, ".tmp");
      MAPPER.writeValue(temp.toFile(), FlexibleSchemaJson.of(schema.getFields()));
      Files.move(temp, entry, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException | RuntimeException e) {
      log.warn("Could not write discovery cache entry {}: {}", entry, e.toString());
    }
  }

  private Path entry(String key) {
    return directory.orElseThrow().resolve(key + ENTRY_SUFFIX);
  }

  private static String fingerprint(SchemaAdjustmentSettings settings) {
    return String.join(
        ",",
        Boolean.toString(settings.deepenArrays()),
        Boolean.toString(settings.removeListNulls()),
        Boolean.toString(settings.null2EmptyArray()),
        Boolean.toString(settings.castDataType()),
        Integer.toString(settings.maxCastingTypeDistance()),
        Boolean.toString(settings.forceCastDataType()),
        Integer.toString(settings.maxForceCastingTypeDistance()),
        Boolean.toString(settings.dropFields()));
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
