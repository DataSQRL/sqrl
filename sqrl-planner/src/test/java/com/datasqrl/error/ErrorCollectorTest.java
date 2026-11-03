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
package com.datasqrl.error;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.ErrorMessage.Severity;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class ErrorCollectorTest {

  @Test
  void givenCollector_whenError_thenRecordsFatalWithoutThrowing() {
    var errors = ErrorCollector.root();

    errors.error("first %s", "problem");
    errors.error("second problem");

    assertThat(errors.hasErrors()).isTrue();
    var messages = new ArrayList<ErrorMessage>();
    errors.forEach(messages::add);
    assertThat(messages)
        .extracting(ErrorMessage::getMessage)
        .containsExactly("first problem", "second problem");
    assertThat(messages).extracting(ErrorMessage::getSeverity).containsOnly(Severity.FATAL);
  }

  @Test
  void givenOnlyErrorsCollector_whenError_thenErrorIsKept() {
    var errors = ErrorCollector.root().onlyErrors();

    errors.error("problem");
    errors.warn("warning");

    var messages = new ArrayList<ErrorMessage>();
    errors.forEach(messages::add);
    assertThat(messages).extracting(ErrorMessage::getMessage).containsExactly("problem");
  }

  @Test
  void givenCollector_whenFatal_thenThrows() {
    var errors = ErrorCollector.root();

    assertThatThrownBy(() -> errors.fatal("problem")).isInstanceOf(CollectedException.class);
    assertThat(errors.hasErrors()).isTrue();
  }

  @Test
  void givenRecordedErrors_whenAbortOnErrors_thenThrowsFirstErrorWithoutRecordingAgain() {
    var errors = ErrorCollector.root();
    errors.warn("warning");
    errors.error("first problem");
    errors.error("second problem");

    assertThatThrownBy(errors::abortOnErrors)
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("first problem");
    var messages = new ArrayList<ErrorMessage>();
    errors.forEach(messages::add);
    assertThat(messages).hasSize(3);
  }

  @Test
  void givenOnlyWarnings_whenAbortOnErrors_thenDoesNotThrow() {
    var errors = ErrorCollector.root();
    errors.warn("warning");

    errors.abortOnErrors();

    assertThat(errors.hasErrors()).isFalse();
  }
}
