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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AvroTableSchemaFactoryTest {

  private AvroTableSchemaFactory factory;

  @BeforeEach
  void setUp() {
    factory = new AvroTableSchemaFactory();
  }

  @Test
  void givenDirectPropertyKeySetToTrue_whenGetLegacyTimestampMapping_thenReturnsTrue() {
    // Given
    var tableProps = new HashMap<String, String>();
    tableProps.put("avro.timestamp_mapping.legacy", "true");

    // When
    var result = factory.getLegacyTimestampMapping(tableProps);

    // Then
    assertThat(result).isTrue();
  }

  @Test
  void givenDirectPropertyKeySetToFalse_whenGetLegacyTimestampMapping_thenReturnsFalse() {
    // Given
    var tableProps = new HashMap<String, String>();
    tableProps.put("avro.timestamp_mapping.legacy", "false");

    // When
    var result = factory.getLegacyTimestampMapping(tableProps);

    // Then
    assertThat(result).isFalse();
  }

  @Test
  void givenValuePrefixedPropertyKeySetToTrue_whenGetLegacyTimestampMapping_thenReturnsTrue() {
    // Given
    var tableProps = new HashMap<String, String>();
    tableProps.put("value.avro.timestamp_mapping.legacy", "true");

    // When
    var result = factory.getLegacyTimestampMapping(tableProps);

    // Then
    assertThat(result).isTrue();
  }

  @Test
  void givenNoPropertiesButAvroConfluentKey_whenGetLegacyTimestampMapping_thenReturnsTrue() {
    // Given
    var tableProps = new HashMap<String, String>();
    tableProps.put("avro-confluent.url", "registry");

    // When
    var result = factory.getLegacyTimestampMapping(tableProps);

    // Then
    assertThat(result).isTrue();
  }

  @Test
  void givenEmptyTableProps_whenGetLegacyTimestampMapping_thenReturnsFalse() {
    // Given
    var tableProps = new HashMap<String, String>();

    // When
    var result = factory.getLegacyTimestampMapping(tableProps);

    // Then
    assertThat(result).isFalse();
  }
}
