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

import com.datasqrl.cli.output.OutputFormatter;
import java.nio.file.Path;
import java.util.Optional;

public abstract class TestResult {

  public abstract String getTestName();

  public abstract boolean isSuccess();

  public abstract int exitCode();

  public abstract void printDetails(OutputFormatter formatter, Optional<Path> testDir);

  abstract static class NamedResult extends TestResult {

    final String snapshotName;

    NamedResult(String snapshotName) {
      this.snapshotName = snapshotName;
    }

    @Override
    public String getTestName() {
      return snapshotName.replace(".snapshot", "");
    }
  }

  public static class SnapshotOk extends NamedResult {

    public SnapshotOk(String testName) {
      super(testName);
    }

    @Override
    public boolean isSuccess() {
      return true;
    }

    @Override
    public int exitCode() {
      return 0;
    }

    @Override
    public void printDetails(OutputFormatter formatter, Optional<Path> testDir) {}
  }

  public static class SnapshotCreate extends NamedResult {

    public SnapshotCreate(String testName) {
      super(testName);
    }

    @Override
    public boolean isSuccess() {
      return false;
    }

    @Override
    public int exitCode() {
      return 1;
    }

    @Override
    public void printDetails(OutputFormatter formatter, Optional<Path> testDir) {
      formatter.warning("Snapshot created for test: " + getTestName());
      formatter.warning("Rerun to verify.");
    }
  }

  public static class SnapshotMissing extends NamedResult {

    public SnapshotMissing(String testName) {
      super(testName);
    }

    @Override
    public boolean isSuccess() {
      return false;
    }

    @Override
    public int exitCode() {
      return 1;
    }

    @Override
    public void printDetails(OutputFormatter formatter, Optional<Path> testDir) {
      formatter.error("Snapshot on filesystem but not in result: " + getTestName());
    }
  }

  public static class SnapshotMismatch extends NamedResult {

    private final String expectedPath;
    private final String actualPath;

    public SnapshotMismatch(String testName, String expectedPath, String actualPath) {
      super(testName);
      this.expectedPath = expectedPath;
      this.actualPath = actualPath;
    }

    @Override
    public boolean isSuccess() {
      return false;
    }

    @Override
    public int exitCode() {
      return 1;
    }

    @Override
    public void printDetails(OutputFormatter formatter, Optional<Path> testDir) {
      var testName = getTestName();

      var testFileName = testName + ".graphql";
      var testFile =
          testDir
              .map(formatter::relativizeFromCliRoot)
              .map(p -> p.resolve(testFileName))
              .map(Path::toString)
              .orElse(testFileName);

      formatter.failureDetails(testName, testFile, expectedPath, actualPath);
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
      return new Failure("Flink job failed", cause);
    }

    public static Failure jdbc(Throwable cause) {
      return new Failure("JDBC validation failed", cause);
    }

    @Override
    public String getTestName() {
      return msg;
    }

    @Override
    public boolean isSuccess() {
      return false;
    }

    @Override
    public int exitCode() {
      return 2;
    }

    @Override
    public void printDetails(OutputFormatter formatter, Optional<Path> testDir) {
      formatter.error(msg);
      if (cause != null) {
        cause.printStackTrace();
      }
    }
  }
}
