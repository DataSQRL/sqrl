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
package com.datasqrl.io.schema.avro;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.SchemaConversionResult;
import com.datasqrl.io.schema.TableSchemaFactory;
import com.datasqrl.util.BaseFileUtil;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.flink.formats.avro.AvroFormatOptions;

@AutoService(TableSchemaFactory.class)
public class AvroTableSchemaFactory implements TableSchemaFactory {

  private static final Set<String> SCHEMA_EXTENSION = Set.of("avsc");
  private static final String SCHEMA_TYPE = "avro";
  private static final String LEGACY_TIMESTAMP_MAPPING_KEY =
      SCHEMA_TYPE + '.' + AvroFormatOptions.AVRO_TIMESTAMP_LEGACY_MAPPING.key();

  @Override
  public SchemaConversionResult convert(
      Path location, Map<String, String> tableProps, ErrorCollector errors) {
    var schemaDefinition = BaseFileUtil.readFile(location);
    errors = errors.withScript(location, schemaDefinition);
    Schema schema;
    try {
      schema = new Schema.Parser().parse(schemaDefinition);
    } catch (Exception e) {
      throw errors.exception(ErrorCode.SCHEMA_ERROR, "Could not parse schema: %s", e);
    }

    var legacyTimestampMapping = getLegacyTimestampMapping(tableProps);
    var converter = new AvroToRelDataTypeConverter(errors, legacyTimestampMapping);

    return new SchemaConversionResult(converter.convert(schema), Map.of());
  }

  @Override
  public String getType() {
    return SCHEMA_TYPE;
  }

  @Override
  public Set<String> getExtensions() {
    return SCHEMA_EXTENSION;
  }

  /**
   * Extracts the legacy timestamp mapping setting from table properties.
   *
   * <p>Checks for the property in the following order:
   *
   * <ol>
   *   <li>Direct property key
   *   <li>Value-prefixed property key
   *   <li>If not found and any property key contains "avro-confluent", returns true
   *   <li>Otherwise returns false
   * </ol>
   *
   * @param tableProps the table properties to extract the setting from
   * @return true if legacy timestamp mapping should be used, false otherwise
   */
  boolean getLegacyTimestampMapping(Map<String, String> tableProps) {
    var legacyTimestampMappingStr = tableProps.get(LEGACY_TIMESTAMP_MAPPING_KEY);

    if (legacyTimestampMappingStr == null) {
      legacyTimestampMappingStr = tableProps.get("value." + LEGACY_TIMESTAMP_MAPPING_KEY);
    }

    // avro-confluent uses legacy no matter what
    if (legacyTimestampMappingStr == null
        && tableProps.keySet().stream().anyMatch(key -> key.contains("avro-confluent"))) {
      return true;
    }

    return Boolean.parseBoolean(legacyTimestampMappingStr);
  }
}
