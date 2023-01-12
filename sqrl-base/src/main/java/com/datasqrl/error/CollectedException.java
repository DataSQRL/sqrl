package com.datasqrl.error;

public class CollectedException extends RuntimeException {

  public CollectedException(Exception cause) {
    super("Collected exception",cause);
  }

}
