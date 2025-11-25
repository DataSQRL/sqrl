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

record AnsiColors(boolean batchMode) {

  String reset() {
    return batchMode ? "" : "\u001B[0m";
  }

  String bold() {
    return batchMode ? "" : "\u001B[1m";
  }

  String green() {
    return batchMode ? "" : "\u001B[32m";
  }

  String boldGreen() {
    return batchMode ? "" : "\u001B[1;32m";
  }

  String red() {
    return batchMode ? "" : "\u001B[31m";
  }

  String boldRed() {
    return batchMode ? "" : "\u001B[1;31m";
  }

  String yellow() {
    return batchMode ? "" : "\u001B[33m";
  }

  String boldYellow() {
    return batchMode ? "" : "\u001B[1;33m";
  }

  String cyan() {
    return batchMode ? "" : "\u001B[36m";
  }

  String boldCyan() {
    return batchMode ? "" : "\u001B[1;36m";
  }
}
