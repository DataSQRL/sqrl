package com.datasqrl.error;

public class NotYetImplementedException extends RuntimeException {

  public NotYetImplementedException(String message) {
    super(message);
  }

  public static void trigger(String msg) {
    throw new NotYetImplementedException(msg);
  }
}
