package com.datasqrl.config.error;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import java.util.Optional;
import lombok.Getter;

public interface ErrorMessage {

  String getMessage();

  Severity getSeverity();

  ErrorLocation getLocation();

  SourceMap getSourceMap();

  Optional<ErrorCode> getErrorCode();

  @JsonIgnore
  default boolean isFatal() {
    return getSeverity() == Severity.FATAL;
  }

  @JsonIgnore
  default boolean isWarning() {
    return getSeverity() == Severity.WARN;
  }

  @JsonIgnore
  default boolean isNotice() {
    return getSeverity() == Severity.NOTICE;
  }

  @JsonIgnore
  default String toStringNoSeverity() {
    String loc = getLocation().toString();
    if (!Strings.isNullOrEmpty(loc)) {
      loc += ": ";
    }
    return loc + getMessage();
  }

  enum Severity {
    NOTICE, WARN, FATAL
  }

  @Getter
  class Implementation implements ErrorMessage {

    private final Optional<ErrorCode> errorCode;
    private final String message;
    private final ErrorLocation location;
    private final Severity severity;
    private final SourceMap sourceMap;

    public Implementation(String message, ErrorLocation location, Severity severity,
        SourceMap sourceMap) {
      this(Optional.empty(), message, location, severity, sourceMap);
    }

    public Implementation(Optional<ErrorCode> errorCode, String message, ErrorLocation location,
        Severity severity, SourceMap sourceMap) {
      this.errorCode = errorCode;
      this.message = message;
      this.location = location;
      this.severity = severity;
      this.sourceMap = sourceMap;
    }

    @Override
    public String toString() {
      return "[" + severity + "] " + toStringNoSeverity();
    }

  }

}
