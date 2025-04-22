package com.datasqrl.error;

import java.io.PrintStream;
import java.io.PrintWriter;

public class CollectedException extends RuntimeException {

  public CollectedException(Throwable cause) {
    super("Collected exception",cause);
  }

  public boolean isInternalError() {
    if (getCause() instanceof NullPointerException) {
		return true;
	}
    return getMessage() == null || getMessage().trim().isEmpty();
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
