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
