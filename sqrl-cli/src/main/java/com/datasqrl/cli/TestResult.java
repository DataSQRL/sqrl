/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.cli;

public abstract class TestResult {

  public abstract void print();

  public abstract int exitCode();

  void printGreen(String line) {
    System.out.println("\u001B[32m" + line + "\u001B[0m");
  }

  void printRed(String line) {
    System.err.println("\u001B[31m" + line + "\u001B[0m");
  }

  abstract static class NamedResult extends TestResult {

    final String snapshotName;

    NamedResult(String snapshotName) {
      this.snapshotName = snapshotName;
    }
  }

  public static class SnapshotOk extends NamedResult {

    public SnapshotOk(String testName) {
      super(testName);
    }

    @Override
    public void print() {
      printGreen("Snapshot OK for " + snapshotName);
    }

    @Override
    public int exitCode() {
      return 0;
    }
  }

  public static class SnapshotCreate extends NamedResult {

    public SnapshotCreate(String testName) {
      super(testName);
    }

    @Override
    public void print() {
      printGreen("Snapshot created for test: " + snapshotName);
      printGreen("Rerun to verify.");
    }

    @Override
    public int exitCode() {
      return 1;
    }
  }

  public static class SnapshotMissing extends NamedResult {

    public SnapshotMissing(String testName) {
      super(testName);
    }

    @Override
    public void print() {
      printRed("Snapshot on filesystem but not in result: " + snapshotName);
    }

    @Override
    public int exitCode() {
      return 1;
    }
  }

  public static class SnapshotMismatch extends NamedResult {

    private final String expected;
    private final String actual;

    public SnapshotMismatch(String testName, String expected, String actual) {
      super(testName);
      this.expected = expected;
      this.actual = actual;
    }

    @Override
    public void print() {
      printRed("Snapshot mismatch for test: " + snapshotName);
      printRed("Expected: " + expected);
      printRed("Actual  : " + actual);
    }

    @Override
    public int exitCode() {
      return 1;
    }
  }

  public static class Failure extends TestResult {

    private final String msg;
    private final Throwable cause;

    public Failure(String msg, Throwable cause) {
      this.msg = msg;
      this.cause = cause;
    }

    public static Failure flink(Throwable cause) {
      return new Failure("Flink job failed to start", cause);
    }

    public static Failure jdbc(Throwable cause) {
      return new Failure("Failed to validate JDBC views", cause);
    }

    @Override
    public void print() {
      printRed(msg);
      if (cause != null) {
        cause.printStackTrace();
      }
    }

    @Override
    public int exitCode() {
      return 2;
    }
  }
}
