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
package com.datasqrl.planner.parser;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;

/** The main exception for parser errors */
public class StatementParserException extends RuntimeException {

  ErrorLabel errorLabel;
  FileLocation fileLocation;

  public StatementParserException(FileLocation fileLocation, Exception e) {
    this(fileLocation, e, e.getMessage());
  }

  public StatementParserException(String message, Object... args) {
    this(ErrorLabel.GENERIC, FileLocation.START, message, args);
  }

  public StatementParserException(FileLocation fileLocation, Exception e, String message) {
    super(message, e);
    this.fileLocation = fileLocation;
    this.errorLabel = ErrorLabel.GENERIC;
  }

  public StatementParserException(
      ErrorLabel errorLabel, FileLocation fileLocation, String message, Object... args) {
    super(args.length == 0 ? message : message.formatted(args));
    this.fileLocation = fileLocation;
    this.errorLabel = errorLabel;
  }

  public static void checkFatal(
      boolean expression, ErrorLabel errorLabel, String message, Object... args) {
    checkFatal(expression, FileLocation.START, errorLabel, message, args);
  }

  public static void checkFatal(
      boolean expression,
      FileLocation fileLocation,
      ErrorLabel errorLabel,
      String message,
      Object... args) {
    if (!expression) {
      throw new StatementParserException(errorLabel, fileLocation, message, args);
    }
  }
}
