package com.datasqrl;

public class SnapshotCreationException extends Exception {
  private final String testName;

  public SnapshotCreationException(String testName) {
    this.testName = testName;
  }

  public String getTestName() {
    return testName;
  }
}
