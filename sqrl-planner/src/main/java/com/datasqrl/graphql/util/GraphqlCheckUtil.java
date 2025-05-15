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
package com.datasqrl.graphql.util;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.parser.StatementParserException;
import graphql.language.SourceLocation;

public class GraphqlCheckUtil {

  public static void checkState(
      boolean check, SourceLocation sourceLocation, String message, Object... args) {
    if (!check) {
      throw createThrowable(sourceLocation, message, args);
    }
  }

  public static StatementParserException createUnknownThrowable(String message, Object... args) {
    return createThrowable(new SourceLocation(0, 0), message, args);
  }

  public static StatementParserException createThrowable(
      SourceLocation sourceLocation, String message, Object... args) {
    return new StatementParserException(
        ErrorLabel.GENERIC, toParserPos(sourceLocation), message, args);
  }

  public static FileLocation toParserPos(SourceLocation sourceLocation) {
    if (sourceLocation == null) {
      return new FileLocation(0, 0);
    }
    return new FileLocation(sourceLocation.getLine(), sourceLocation.getColumn());
  }
}
