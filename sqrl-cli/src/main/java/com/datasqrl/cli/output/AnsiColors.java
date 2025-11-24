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

public class AnsiColors {

  private final boolean batchMode;

  public AnsiColors(boolean batchMode) {
    this.batchMode = batchMode;
  }

  public String reset() {
    return batchMode ? "" : "\u001B[0m";
  }

  public String bold() {
    return batchMode ? "" : "\u001B[1m";
  }

  public String green() {
    return batchMode ? "" : "\u001B[32m";
  }

  public String boldGreen() {
    return batchMode ? "" : "\u001B[1;32m";
  }

  public String red() {
    return batchMode ? "" : "\u001B[31m";
  }

  public String boldRed() {
    return batchMode ? "" : "\u001B[1;31m";
  }

  public String yellow() {
    return batchMode ? "" : "\u001B[33m";
  }

  public String boldYellow() {
    return batchMode ? "" : "\u001B[1;33m";
  }

  public String cyan() {
    return batchMode ? "" : "\u001B[36m";
  }

  public String boldCyan() {
    return batchMode ? "" : "\u001B[1;36m";
  }
}
