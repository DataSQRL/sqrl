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

import java.time.LocalDateTime;

public class NoOutputFormatter implements OutputFormatter {

  @Override
  public void separator() {}

  @Override
  public void newline() {}

  @Override
  public void header(String title) {}

  @Override
  public void sectionHeader(String title) {}

  @Override
  public void phaseStart(String phaseName) {}

  @Override
  public void info(String message) {}

  @Override
  public void warning(String message) {}

  @Override
  public void error(String message) {}

  @Override
  public void success(String message) {}

  @Override
  public void helpText(String message) {}

  @Override
  public void helpLink(String label, String url) {}

  @Override
  public void buildStatus(boolean success, long durationMillis, LocalDateTime finishedAt) {}

  @Override
  public void testResult(String testName, boolean success) {}

  @Override
  public void testSummary(int totalTests, int failures) {}

  @Override
  public void failureDetails(
      String testName, String testFile, String expectedFile, String actualFile) {}
}
