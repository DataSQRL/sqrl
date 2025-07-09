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
package com.datasqrl.error;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;

public class CollectedException extends RuntimeException {

  public CollectedException(Throwable cause) {
    super("Collected exception", cause);
  }

  // TODO: use this in ErrorCollector to pinpoint the actual error
  public static CollectedException withTrimmedStackTrace(Throwable cause) {
    var orig = cause.getStackTrace();
    StackTraceElement[] trimmed = Arrays.copyOfRange(orig, 4, orig.length);
    cause.setStackTrace(trimmed);

    return new CollectedException(cause);
  }

  public boolean isInternalError() {
    if (getCause() instanceof NullPointerException) {
      return true;
    }
    return getMessage() == null || getMessage().trim().isEmpty();
  }

  @Override
  public String getMessage() {
    return getCause().getMessage();
  }

  @Override
  public void printStackTrace() {
    getCause().printStackTrace();
  }

  @Override
  public void printStackTrace(PrintStream s) {
    getCause().printStackTrace(s);
  }

  @Override
  public void printStackTrace(PrintWriter s) {
    getCause().printStackTrace(s);
  }
}
