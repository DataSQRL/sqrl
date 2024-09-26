package com.datasqrl;

public class SnapshotOkException extends Exception {
  private final String testName;

  public SnapshotOkException(String testName) {
    super("Snapshot created for test: " + testName + ". Rerun to verify.");
    this.testName = testName;
  }

  public String getTestName() {
    return testName;
  }
}
