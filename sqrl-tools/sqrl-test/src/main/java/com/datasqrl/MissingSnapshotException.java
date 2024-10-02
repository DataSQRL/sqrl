package com.datasqrl;

public class MissingSnapshotException extends Exception {
  private final String testName;

  public MissingSnapshotException(String testName) {
    this.testName = testName;
  }

  public String getTestName() {
    return testName;
  }
}
