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
package com.datasqrl.cli.output;

import java.nio.file.Path;
import java.time.LocalDateTime;

public interface OutputFormatter {

  void separator();

  void newline();

  void header(String title);

  void sectionHeader(String title);

  void phaseStart(String phaseName);

  void info(String message);

  void warning(String message);

  void error(String message);

  void success(String message);

  void helpText(String message);

  void helpLink(String label, String url);

  void buildStatus(boolean success, long durationMillis, LocalDateTime finishedAt);

  void testResult(String testName, boolean success);

  void testSummary(int totalTests, int failures);

  void failureDetails(String testName, String testFile, String expectedFile, String actualFile);

  default Path relativizeFromCliRoot(Path path) {
    return path;
  }
}
