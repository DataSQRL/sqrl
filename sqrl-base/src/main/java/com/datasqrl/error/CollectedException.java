package com.datasqrl.error;

import java.io.PrintStream;
import java.io.PrintWriter;

public class CollectedException extends RuntimeException {

  public CollectedException(Exception cause) {
    super("Collected exception",cause);
  }

  @Override
  public String getMessage() {
    return getCause().getMessage();
  }

  @Override
  public void printStackTrace() {
    getCause().printStackTrace();
  }

  @Override
  public void printStackTrace(PrintStream s) {
    getCause().printStackTrace(s);
  }

  @Override
  public void printStackTrace(PrintWriter s) {
    getCause().printStackTrace(s);
  }
}
