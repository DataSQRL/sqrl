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

import lombok.Getter;

public interface ErrorMessage {

  String getMessage();

  Severity getSeverity();

  ErrorLocation getLocation();

  ErrorLabel getErrorLabel();

  default boolean isFatal() {
    return getSeverity() == Severity.FATAL;
  }

  default boolean isWarning() {
    return getSeverity() == Severity.WARN;
  }

  default boolean isNotice() {
    return getSeverity() == Severity.NOTICE;
  }

  default String toStringNoSeverity() {
    var loc = getLocation().toString();
    if (loc != null && !loc.trim().isEmpty()) {
      loc += ": ";
    }
    return loc + getMessage();
  }

  default RuntimeException asException() {
    return getErrorLabel().toException().apply(getMessage());
  }

  enum Severity {
    NOTICE,
    WARN,
    FATAL
  }

  @Getter
  class Implementation implements ErrorMessage {

    private final ErrorLabel errorLabel;
    private final String message;
    private final ErrorLocation location;
    private final Severity severity;

    public Implementation(String message, ErrorLocation location, Severity severity) {
      this(ErrorLabel.GENERIC, message, location, severity);
    }

    public Implementation(
        ErrorLabel errorLabel, String message, ErrorLocation location, Severity severity) {
      this.errorLabel = errorLabel;
      this.message = message == null ? "" : message;
      this.location = location;
      this.severity = severity;
    }

    @Override
    public String toString() {
      return "[" + severity + "] " + toStringNoSeverity();
    }
  }

  static String getMessage(String msgTemplate, Object... args) {
    if (args == null || args.length == 0) {
      return msgTemplate;
    }
    return msgTemplate.formatted(args);
  }
}
