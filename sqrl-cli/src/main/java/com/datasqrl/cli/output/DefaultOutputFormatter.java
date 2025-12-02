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

import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DefaultOutputFormatter implements OutputFormatter {

  private static final String SEPARATOR =
      "------------------------------------------------------------------------";
  private static final int TEST_NAME_WIDTH = 50;
  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

  private final AnsiColors colors;
  private final PrintStream out;
  private final PrintStream err;

  public DefaultOutputFormatter(boolean batchMode) {
    this(batchMode, System.out, System.err);
  }

  public DefaultOutputFormatter(boolean batchMode, PrintStream out, PrintStream err) {
    this.colors = new AnsiColors(batchMode);
    this.out = out;
    this.err = err;
  }

  @Override
  public void separator() {
    out.println(SEPARATOR);
  }

  @Override
  public void newline() {
    out.println();
  }

  @Override
  public void header(String title) {
    separator();
    out.println(colors.boldCyan() + title + colors.reset());
    separator();
    newline();
  }

  @Override
  public void sectionHeader(String title) {
    newline();
    separator();
    out.println(colors.bold() + title + colors.reset());
    separator();
    newline();
  }

  @Override
  public void phaseStart(String phaseName) {
    out.println(phaseName + " ...");
  }

  @Override
  public void info(String message) {
    out.println(message);
  }

  @Override
  public void warning(String message) {
    out.println(colors.boldYellow() + message + colors.reset());
  }

  @Override
  public void error(String message) {
    err.println(colors.boldRed() + message + colors.reset());
  }

  @Override
  public void success(String message) {
    out.println(colors.boldGreen() + message + colors.reset());
  }

  @Override
  public void helpText(String message) {
    newline();
    out.println(message);
  }

  @Override
  public void helpLink(String label, String url) {
    newline();
    out.println("-> [" + label + "] " + url);
  }

  @Override
  public void buildStatus(boolean success, long durationMillis, LocalDateTime finishedAt) {
    separator();
    if (success) {
      out.println(colors.boldGreen() + "BUILD SUCCESS" + colors.reset());
    } else {
      out.println(colors.boldRed() + "BUILD FAILURE" + colors.reset());
    }
    separator();
    out.println("Total time:  " + formatDuration(durationMillis));
    out.println("Finished at: " + finishedAt.format(TIMESTAMP_FORMAT));
    separator();
  }

  @Override
  public void testResult(String testName, boolean success) {
    var status =
        success
            ? colors.boldGreen() + "SUCCESS" + colors.reset()
            : colors.boldRed() + "FAILURE" + colors.reset();

    var dots = ".".repeat(Math.max(1, TEST_NAME_WIDTH - testName.length()));
    out.println(testName + " " + dots + " " + status);
  }

  @Override
  public void testSummary(int totalTests, int failures) {
    newline();
    out.println("Tests run: " + totalTests + ", Failures: " + failures);
  }

  @Override
  public void failureDetails(
      String testName, String testFile, String expectedFile, String actualFile, String diffFile) {
    out.println("  " + colors.bold() + testName + colors.reset());
    out.println("    Test:     " + testFile);
    out.println("    Expected: " + expectedFile);
    out.println("    Actual:   " + actualFile);
    out.println("    Diff:     " + diffFile);
    newline();
  }

  private String formatDuration(long millis) {
    long seconds = millis / 1000;
    long minutes = seconds / 60;
    seconds = seconds % 60;

    if (minutes > 0) {
      return String.format("%02d:%02d min", minutes, seconds);
    } else {
      return seconds + " s";
    }
  }
}
