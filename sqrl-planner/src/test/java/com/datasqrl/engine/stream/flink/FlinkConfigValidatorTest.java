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
package com.datasqrl.engine.stream.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

class FlinkConfigValidatorTest {

  @Test
  void givenDeploymentKeyInConfig_whenValidate_thenWarnsAboutMisplacement() {
    var errors = ErrorCollector.root();
    var config = Map.<String, Object>of("taskmanager-size", "large");

    FlinkConfigValidator.validate(config, errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isTrue();
    assertThat(warningMessages(errors))
        .anyMatch(msg -> msg.contains("belongs in 'engines.flink.deployment'"));
  }

  @Test
  void givenMultipleDeploymentKeys_whenValidate_thenWarnsForEach() {
    var errors = ErrorCollector.root();
    var config = Map.<String, Object>of("jobmanager-size", "medium", "taskmanager-count", 4);

    FlinkConfigValidator.validate(config, errors);

    var warnings = warningMessages(errors);
    assertThat(warnings).hasSize(2);
    assertThat(warnings).allMatch(msg -> msg.contains("belongs in 'engines.flink.deployment'"));
  }

  @Test
  void givenUnrecognizedKey_whenValidate_thenWarnsAboutUnknownKey() {
    var errors = ErrorCollector.root();
    var config = Map.<String, Object>of("made.up.nonsense", "value");

    FlinkConfigValidator.validate(config, errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isTrue();
    assertThat(warningMessages(errors))
        .anyMatch(msg -> msg.contains("Unrecognized Flink configuration key"));
  }

  @Test
  void givenKeyWithKnownPrefix_whenValidate_thenWarnsWithPrefixHint() {
    var errors = ErrorCollector.root();
    var config = Map.<String, Object>of("state.backend.typo", "rocksdb");

    FlinkConfigValidator.validate(config, errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isTrue();
    assertThat(warningMessages(errors))
        .anyMatch(msg -> msg.contains("prefix") && msg.contains("is valid"));
  }

  @Test
  void givenValidFlinkKeys_whenValidate_thenNoWarnings() {
    var errors = ErrorCollector.root();
    var config =
        Map.<String, Object>of(
            "execution.runtime-mode", "STREAMING",
            "pipeline.name", "my-job");

    FlinkConfigValidator.validate(config, errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenBooleanValueAsString_whenValidate_thenNoWarnings() {
    var errors = ErrorCollector.root();
    var config = Map.<String, Object>of("pipeline.auto-generate-uids", "true");

    FlinkConfigValidator.validate(config, errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenInvalidBooleanValue_whenValidate_thenWarnsAboutType() {
    var errors = ErrorCollector.root();
    var config = Map.<String, Object>of("pipeline.auto-generate-uids", "yes");

    FlinkConfigValidator.validate(config, errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isTrue();
    assertThat(warningMessages(errors)).anyMatch(msg -> msg.contains("Expected type: Boolean"));
  }

  @Test
  void givenInvalidDurationValue_whenValidate_thenWarnsAboutType() {
    var errors = ErrorCollector.root();
    var config = Map.<String, Object>of("execution.checkpointing.interval", "not-a-duration");

    FlinkConfigValidator.validate(config, errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isTrue();
    assertThat(warningMessages(errors)).anyMatch(msg -> msg.contains("Expected type: Duration"));
  }

  @Test
  void givenValidDurationValue_whenValidate_thenNoWarnings() {
    var errors = ErrorCollector.root();
    var config = Map.<String, Object>of("execution.checkpointing.interval", "60s");

    FlinkConfigValidator.validate(config, errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenEmptyConfig_whenValidate_thenNoWarnings() {
    var errors = ErrorCollector.root();

    FlinkConfigValidator.validate(Map.of(), errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenNullConfig_whenValidate_thenNoWarnings() {
    var errors = ErrorCollector.root();

    FlinkConfigValidator.validate(null, errors);

    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  private java.util.List<String> warningMessages(ErrorCollector errors) {
    return StreamSupport.stream(errors.spliterator(), false)
        .map(ErrorMessage::getMessage)
        .collect(Collectors.toList());
  }
}
