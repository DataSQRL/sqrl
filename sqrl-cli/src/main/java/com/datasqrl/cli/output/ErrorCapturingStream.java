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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * An OutputStream that writes to a delegate stream while capturing lines that appear to be errors
 * or exceptions. This allows surfacing important error messages to users when tests fail, similar
 * to how Maven shows errors at the end of a failed build.
 */
public class ErrorCapturingStream extends OutputStream {

  private static final Pattern EXCEPTION_PATTERN =
      Pattern.compile("^(.*Exception|.*Error|Caused by:).*", Pattern.CASE_INSENSITIVE);
  private static final Pattern LOG_ERROR_PATTERN = Pattern.compile("^\\[(WARN|ERROR)].*");
  private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("^\\s+at\\s+.*");

  private final OutputStream delegate;
  private final ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();
  private final List<String> capturedErrors = new ArrayList<>();

  private boolean capturingStackTrace = false;

  public ErrorCapturingStream(OutputStream delegate) {
    this.delegate = delegate;
  }

  @Override
  public void write(int b) throws IOException {
    delegate.write(b);

    if (b == '\n') {
      processLine();
    } else {
      lineBuffer.write(b);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    delegate.write(b, off, len);

    for (int i = off; i < off + len; i++) {
      if (b[i] == '\n') {
        processLine();
      } else {
        lineBuffer.write(b[i]);
      }
    }
  }

  @Override
  public void flush() throws IOException {
    delegate.flush();
  }

  @Override
  public void close() throws IOException {
    if (lineBuffer.size() > 0) {
      processLine();
    }
    delegate.close();
  }

  private void processLine() {
    var line = lineBuffer.toString();
    lineBuffer.reset();

    if (EXCEPTION_PATTERN.matcher(line).matches()) {
      capturedErrors.add(line);
      capturingStackTrace = true;
    } else if (LOG_ERROR_PATTERN.matcher(line).matches()) {
      capturedErrors.add(line);
      capturingStackTrace = false;
    } else if (capturingStackTrace && STACK_TRACE_PATTERN.matcher(line).matches()) {
      capturedErrors.add(line);
    } else {
      capturingStackTrace = false;
    }
  }

  public List<String> getCapturedErrors() {
    return new ArrayList<>(capturedErrors);
  }

  public boolean hasErrors() {
    return !capturedErrors.isEmpty();
  }

  public void clear() {
    capturedErrors.clear();
    capturingStackTrace = false;
  }
}
