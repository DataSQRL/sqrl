/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
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
    String loc = getLocation().toString();
    if (loc == null || loc.trim().isEmpty()) {
      loc += ": ";
    }
    return loc + getMessage();
  }

  default RuntimeException asException() {
    return getErrorLabel().toException().apply(getMessage());
  }

  enum Severity {
    NOTICE, WARN, FATAL
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

    public Implementation(ErrorLabel errorLabel, String message, ErrorLocation location,
        Severity severity) {
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
