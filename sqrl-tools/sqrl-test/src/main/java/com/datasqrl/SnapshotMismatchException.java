package com.datasqrl;

public class SnapshotMismatchException extends Exception {
  private final String testName;
  private final String expected;
  private final String actual;

  public SnapshotMismatchException(String testName, String expected, String actual) {
    super("Snapshot mismatch for test: " + testName);
    this.testName = testName;
    this.expected = expected;
    this.actual = actual;
  }

  public String getTestName() {
    return testName;
  }

  public String getExpected() {
    return expected;
  }

  public String getActual() {
    return actual;
  }
}
