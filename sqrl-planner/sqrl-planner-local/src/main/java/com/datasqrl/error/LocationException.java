package com.datasqrl.error;

import lombok.Getter;

@Getter
public class LocationException extends RuntimeException {

  private final ErrorLabel label;
  private final ErrorLocation location;

  public LocationException(ErrorLabel label, ErrorLocation location, String msg) {
    super(msg);
    this.label = label;
    this.location = location;
  }

  public static void check(boolean condition, ErrorLabel label, ErrorLocation location, String msgTemplate, Object... args) {
    if (!condition) {
      throw new LocationException(label, location, ErrorMessage.getMessage(msgTemplate, args));
    }
  }

}
