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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ErrorCapturingStreamTest {

  private ByteArrayOutputStream delegate;
  private ErrorCapturingStream stream;
  private PrintStream printStream;

  @BeforeEach
  void setUp() {
    delegate = new ByteArrayOutputStream();
    stream = new ErrorCapturingStream(delegate);
    printStream = new PrintStream(stream);
  }

  @Test
  void givenNormalOutput_whenWriting_thenNoErrorsCaptured() {
    printStream.println("This is a normal log line");
    printStream.println("Another normal line");

    assertThat(stream.hasErrors()).isFalse();
    assertThat(stream.getCapturedErrors()).isEmpty();
    assertThat(delegate.toString()).contains("This is a normal log line");
  }

  @Test
  void givenRuntimeException_whenWriting_thenExceptionCaptured() {
    printStream.println("java.lang.RuntimeException: RowTime field should not be null");
    printStream.println(
        "    at org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperator.processElement(WatermarkAssignerOperator.java:137)");
    printStream.println(
        "    at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.pushToOperator(CopyingChainingOutput.java:75)");
    printStream.println("Normal line after stack trace");

    assertThat(stream.hasErrors()).isTrue();
    var errors = stream.getCapturedErrors();
    assertThat(errors).hasSize(3);
    assertThat(errors.get(0)).contains("RuntimeException");
    assertThat(errors.get(1)).contains("at org.apache.flink");
    assertThat(errors.get(2)).contains("at org.apache.flink");
  }

  @Test
  void givenCausedByException_whenWriting_thenCausedByCaptured() {
    printStream.println("java.lang.IllegalStateException: Failed to process");
    printStream.println("    at com.example.MyClass.method(MyClass.java:10)");
    printStream.println("Caused by: java.lang.NullPointerException: value is null");
    printStream.println("    at com.example.OtherClass.getValue(OtherClass.java:20)");

    assertThat(stream.hasErrors()).isTrue();
    var errors = stream.getCapturedErrors();
    assertThat(errors).hasSizeGreaterThanOrEqualTo(4);
    assertThat(errors).anyMatch(e -> e.contains("IllegalStateException"));
    assertThat(errors).anyMatch(e -> e.contains("Caused by:"));
    assertThat(errors).anyMatch(e -> e.contains("NullPointerException"));
  }

  @Test
  void givenErrorClass_whenWriting_thenErrorCaptured() {
    printStream.println("java.lang.OutOfMemoryError: Java heap space");
    printStream.println("    at java.base/java.util.Arrays.copyOf(Arrays.java:3720)");

    assertThat(stream.hasErrors()).isTrue();
    var errors = stream.getCapturedErrors();
    assertThat(errors.get(0)).contains("OutOfMemoryError");
  }

  @Test
  void givenClear_whenClearCalled_thenErrorsCleared() {
    printStream.println("java.lang.RuntimeException: Some error");

    assertThat(stream.hasErrors()).isTrue();

    stream.clear();

    assertThat(stream.hasErrors()).isFalse();
    assertThat(stream.getCapturedErrors()).isEmpty();
  }

  @Test
  void givenLongStackTrace_whenWriting_thenStackTraceTruncated() {
    printStream.println("java.lang.RuntimeException: Test error");
    for (int i = 0; i < 30; i++) {
      printStream.println("    at com.example.Class" + i + ".method(Class.java:" + i + ")");
    }

    var errors = stream.getCapturedErrors();
    assertThat(errors).hasSizeLessThanOrEqualTo(22);
    assertThat(errors).anyMatch(e -> e.contains("truncated"));
  }

  @Test
  void givenWriteByteArray_whenWriting_thenContentPassedToDelegate() {
    var content = "Some content\n";
    printStream.print(content);
    printStream.flush();

    assertThat(delegate.toString()).isEqualTo(content);
  }

  @Test
  void givenWarnLogLine_whenWriting_thenWarnCaptured() {
    printStream.println("[WARN] The rowtime column 'event_time' for this table is nullable");
    printStream.println("Normal line after warning");

    assertThat(stream.hasErrors()).isTrue();
    var errors = stream.getCapturedErrors();
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("[WARN]");
    assertThat(errors.get(0)).contains("nullable");
  }

  @Test
  void givenErrorLogLine_whenWriting_thenErrorCaptured() {
    printStream.println("[ERROR] Failed to process record");
    printStream.println("Some context information");

    assertThat(stream.hasErrors()).isTrue();
    var errors = stream.getCapturedErrors();
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("[ERROR]");
  }

  @Test
  void givenMixedWarningsAndExceptions_whenWriting_thenAllCaptured() {
    printStream.println("[WARN] This is a warning");
    printStream.println("Normal output");
    printStream.println("java.lang.RuntimeException: Test error");
    printStream.println("    at com.example.Class.method(Class.java:10)");
    printStream.println("[ERROR] Another error log");

    assertThat(stream.hasErrors()).isTrue();
    var errors = stream.getCapturedErrors();
    assertThat(errors).hasSize(4);
    assertThat(errors.get(0)).contains("[WARN]");
    assertThat(errors.get(1)).contains("RuntimeException");
    assertThat(errors.get(2)).contains("at com.example");
    assertThat(errors.get(3)).contains("[ERROR]");
  }
}
