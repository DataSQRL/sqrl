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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaFormatConfigurerTest {

  private static final String TIMESTAMP_FORMAT = "flexible-json.timestamp-format.standard";

  private final ErrorCollector errors = ErrorCollector.root();
  private final KafkaFormatConfigurer formats = new KafkaFormatConfigurer(errors);

  @Test
  void givenFormatOnly_whenConfigureUnkeyedTable_thenKeepsTheSingleFormat() {
    var options = defaultOptions();

    var valueFormat = formats.configure(options, "Reading", false, true);

    assertThat(valueFormat).isEqualTo("flexible-json");
    assertThat(options)
        .containsEntry("format", "flexible-json")
        .containsEntry(TIMESTAMP_FORMAT, "ISO-8601")
        .doesNotContainKeys("key.format", "value.format");
  }

  @Test
  void givenFormatOnly_whenConfigureKeyedTable_thenUsesItForKeyAndValue() {
    var options = defaultOptions();

    formats.configure(options, "Reading", true, true);

    assertThat(options)
        .containsEntry("key.format", "flexible-json")
        .containsEntry("value.format", "flexible-json")
        .containsEntry("key." + TIMESTAMP_FORMAT, "ISO-8601")
        .containsEntry("value." + TIMESTAMP_FORMAT, "ISO-8601")
        .doesNotContainKeys("format", TIMESTAMP_FORMAT);
  }

  @Test
  void givenExplicitValueFormatOption_whenConfigureKeyedTable_thenKeepsItOverTheDefault() {
    var options = defaultOptions("value." + TIMESTAMP_FORMAT, "SQL");

    formats.configure(options, "Reading", true, true);

    assertThat(options)
        .containsEntry("key." + TIMESTAMP_FORMAT, "ISO-8601")
        .containsEntry("value." + TIMESTAMP_FORMAT, "SQL");
  }

  @Test
  void givenDifferentKeyAndValueFormats_whenConfigureKeyedMutation_thenUsesThemAsGiven() {
    var options = defaultOptions("key.format", "json", "value.format", "avro");

    var valueFormat = formats.configure(options, "Reading", true, true);

    assertThat(valueFormat).isEqualTo("avro");
    assertThat(options)
        .containsEntry("key.format", "json")
        .containsEntry("value.format", "avro")
        .doesNotContainKeys(
            "format", TIMESTAMP_FORMAT, "key." + TIMESTAMP_FORMAT, "value." + TIMESTAMP_FORMAT);
    assertThat(warnings())
        .extracting(ErrorMessage::getMessage)
        .singleElement()
        .asString()
        .contains("'Reading'", "'avro' format for its value", "writes mutations as JSON");
  }

  @Test
  void givenNonJsonFormat_whenConfigureNonMutationTable_thenDoesNotWarn() {
    var options = defaultOptions("key.format", "json", "value.format", "avro");

    formats.configure(options, "Reading", true, false);

    assertThat(warnings()).isEmpty();
  }

  @Test
  void givenSameKeyAndValueFormat_whenConfigureTwice_thenNoticesOnceThatItCanBeSimplified() {
    formats.configure(
        defaultOptions("key.format", "json", "value.format", "json"), "A", true, true);
    var options = defaultOptions("key.format", "json", "value.format", "json");

    formats.configure(options, "B", true, true);

    assertThat(options).containsEntry("key.format", "json").containsEntry("value.format", "json");
    assertThat(notices())
        .extracting(ErrorMessage::getMessage)
        .singleElement()
        .asString()
        .contains("'json'", "simplified to 'format'");
  }

  @Test
  void givenValueFormatOnly_whenConfigureKeyedTable_thenKeyFallsBackToFormat() {
    var options = defaultOptions("value.format", "json");

    formats.configure(options, "Reading", true, true);

    assertThat(options)
        .containsEntry("key.format", "flexible-json")
        .containsEntry("key." + TIMESTAMP_FORMAT, "ISO-8601")
        .containsEntry("value.format", "json")
        .doesNotContainKeys("format", "value." + TIMESTAMP_FORMAT);
  }

  @Test
  void givenValueFormat_whenConfigureUnkeyedTable_thenReplacesTheDefaultFormat() {
    var options = defaultOptions("value.format", "json");

    formats.configure(options, "Reading", false, true);

    assertThat(options)
        .containsEntry("value.format", "json")
        .doesNotContainKeys("format", "key.format", TIMESTAMP_FORMAT, "value." + TIMESTAMP_FORMAT);
  }

  @Test
  void givenKeyFormat_whenConfigureUnkeyedTable_thenDropsIt() {
    var options = defaultOptions("key.format", "json");

    formats.configure(options, "Reading", false, true);

    assertThat(options)
        .containsEntry("format", "flexible-json")
        .containsEntry(TIMESTAMP_FORMAT, "ISO-8601")
        .doesNotContainKeys("key.format", "value.format");
  }

  @Test
  void givenNoFormat_whenConfigure_thenFailsWithATableSpecificMessage() {
    var options = new HashMap<String, String>(Map.of("topic", "Reading"));

    assertThatThrownBy(() -> formats.configure(options, "Reading", false, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Need to configure 'format' or 'value.format' for Kafka table 'Reading'");
  }

  @Test
  void givenValueFormatAndFormat_whenGetValueFormat_thenPrefersValueFormat() {
    assertThat(KafkaFormatConfigurer.valueFormat(defaultOptions("value.format", "json")))
        .isEqualTo("json");
    assertThat(KafkaFormatConfigurer.valueFormat(defaultOptions())).isEqualTo("flexible-json");
    assertThat(KafkaFormatConfigurer.valueFormat(Map.of())).isNull();
  }

  /** The default connector options, extended by the given key and value pairs. */
  private static Map<String, String> defaultOptions(String... extraOptions) {
    var options = new HashMap<>(Map.of("format", "flexible-json", TIMESTAMP_FORMAT, "ISO-8601"));
    for (int i = 0; i < extraOptions.length; i += 2) {
      options.put(extraOptions[i], extraOptions[i + 1]);
    }
    return options;
  }

  private List<ErrorMessage> warnings() {
    return errors.getErrors().stream().filter(ErrorMessage::isWarning).toList();
  }

  private List<ErrorMessage> notices() {
    return errors.getErrors().stream().filter(ErrorMessage::isNotice).toList();
  }
}
