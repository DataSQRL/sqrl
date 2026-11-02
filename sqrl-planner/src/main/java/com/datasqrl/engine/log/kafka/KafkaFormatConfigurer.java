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
package com.datasqrl.engine.log.kafka;

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkrunner.format.json.FlexibleJsonFormat;
import com.datasqrl.planner.tables.FlinkConnectorConfigWrapper;
import jakarta.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/**
 * Configures the key and value formats of the Kafka tables that the {@link KafkaLogEngine} manages.
 * An explicit 'key.format' or 'value.format' is kept as given and takes precedence over 'format',
 * which comes from the default connector configuration.
 */
@RequiredArgsConstructor
class KafkaFormatConfigurer {

  /** The formats of the JSON the server writes for mutations. */
  private static final Set<String> JSON_FORMATS = Set.of("json", FlexibleJsonFormat.FORMAT_NAME);

  /** Formats already reported as simplifiable to 'format', so each is reported once. */
  private final Set<String> simplifiableFormats = new HashSet<>();

  private final ErrorCollector errors;

  /**
   * Returns the value format of the connector options: 'value.format', otherwise 'format', or
   * {@code null} if neither is set.
   */
  static @Nullable String valueFormat(Map<String, String> options) {
    return options.getOrDefault(
        FlinkConnectorConfigWrapper.VALUE_FORMAT_KEY,
        options.get(FlinkConnectorConfigWrapper.FORMAT_KEY));
  }

  /**
   * Sets the key and value formats in the connector options. The options of 'format' are moved to
   * each side that uses it, without overriding options that are already set for that side.
   *
   * @return the value format
   */
  String configure(
      Map<String, String> options, String tableName, boolean hasMessageKey, boolean isMutation) {
    var format = options.remove(FlinkConnectorConfigWrapper.FORMAT_KEY);
    var explicitKeyFormat = options.remove(FlinkConnectorConfigWrapper.KEY_FORMAT_KEY);
    var explicitValueFormat = options.get(FlinkConnectorConfigWrapper.VALUE_FORMAT_KEY);
    var valueFormat = explicitValueFormat != null ? explicitValueFormat : format;
    checkArgument(
        valueFormat != null && !valueFormat.isBlank(),
        "Need to configure 'format' or 'value.format' for Kafka table '%s'",
        tableName);
    if (explicitKeyFormat != null
        && explicitKeyFormat.equals(explicitValueFormat)
        && simplifiableFormats.add(explicitKeyFormat)) {
      errors.notice(
          "The Kafka connector options set both 'key.format' and 'value.format' to '%s', which"
              + " can be simplified to 'format'",
          explicitKeyFormat);
    }

    var formatOptions =
        format == null
            ? Map.<String, String>of()
            : options.entrySet().stream()
                .filter(option -> option.getKey().startsWith(format + "."))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    formatOptions.keySet().forEach(options::remove);

    if (!hasMessageKey && explicitValueFormat == null) {
      // A single value format, which is the plain 'format' configuration; 'key.format' is dropped
      // since Flink only accepts it together with 'key.fields'
      options.put(FlinkConnectorConfigWrapper.FORMAT_KEY, valueFormat);
      options.putAll(formatOptions);
      warnAboutNonJsonFormat(tableName, isMutation, "value", valueFormat);
      return valueFormat;
    }

    var keyFormat =
        hasMessageKey
            ? Optional.ofNullable(explicitKeyFormat)
                .or(() -> Optional.ofNullable(format))
                .orElse(valueFormat)
            : null;
    if (keyFormat != null) {
      options.put(FlinkConnectorConfigWrapper.KEY_FORMAT_KEY, keyFormat);
      warnAboutNonJsonFormat(tableName, isMutation, "key", keyFormat);
    }
    options.put(FlinkConnectorConfigWrapper.VALUE_FORMAT_KEY, valueFormat);
    warnAboutNonJsonFormat(tableName, isMutation, "value", valueFormat);
    var keyUsesFormat = keyFormat != null && keyFormat.equals(format);
    var valueUsesFormat = valueFormat.equals(format);
    formatOptions.forEach(
        (key, value) -> {
          if (keyUsesFormat) {
            options.putIfAbsent("key." + key, value);
          }
          if (valueUsesFormat) {
            options.putIfAbsent("value." + key, value);
          }
        });
    return valueFormat;
  }

  /** The server writes mutations as JSON, so other formats on mutation topics cannot be read. */
  private void warnAboutNonJsonFormat(
      String tableName, boolean isMutation, String side, String format) {
    if (isMutation && !JSON_FORMATS.contains(format.toLowerCase())) {
      errors.warn(
          "Kafka mutation table '%s' uses the '%s' format for its %s, but the server writes"
              + " mutations as JSON",
          tableName, format, side);
    }
  }
}
