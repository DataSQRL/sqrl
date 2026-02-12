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
package com.datasqrl.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JBangRunnerTest {

  @TempDir private Path tempDir;

  @Test
  void given_runningFromClassesDirectory_when_resolveCliJarPath_then_returnsEmpty() {
    var result = JBangRunner.resolveCliJarPath();
    assertThat(result).isEmpty();
  }

  @Test
  void given_runningFromClassesDirectory_when_resolveClasspath_then_throwsIllegalState() {
    var runner = JBangRunner.create();

    assertThatThrownBy(runner::resolveClasspath)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot resolve sqrl-cli.jar path");
  }

  @Test
  void given_withClasspath_when_resolveClasspath_then_returnsOverride() {
    var runner = JBangRunner.withClasspath("/some/custom.jar");

    assertThat(runner.resolveClasspath()).isEqualTo("/some/custom.jar");
  }

  @Test
  void given_disabledRunner_when_isJBangAvailable_then_returnsFalse() {
    var runner = JBangRunner.disabled();

    assertThat(runner.isJBangAvailable()).isFalse();
  }

  @Test
  void given_disabledRunner_when_exportFatJar_then_doesNothing() {
    var runner = JBangRunner.disabled();
    var src = tempDir.resolve("Dummy.java");
    var target = tempDir.resolve("Dummy.jar");

    assertThatCode(() -> runner.exportFatJar(List.of(src), target)).doesNotThrowAnyException();
    assertThat(target).doesNotExist();
  }

  @Test
  void given_jbangUnavailable_when_exportFatJar_then_skipsWithoutError() {
    var runner = JBangRunner.create();
    assumeThat(runner.isJBangAvailable()).isFalse();

    var src = tempDir.resolve("Dummy.java");
    var target = tempDir.resolve("Dummy.jar");

    assertThatCode(() -> runner.exportFatJar(List.of(src), target)).doesNotThrowAnyException();
    assertThat(target).doesNotExist();
  }

  @Test
  void given_noClasspathOverride_when_exportFatJar_then_throwsBecauseNoJar() {
    var runner = JBangRunner.create();
    assumeThat(runner.isJBangAvailable()).isTrue();

    var src = tempDir.resolve("Dummy.java");
    var target = tempDir.resolve("Dummy.jar");

    assertThatThrownBy(() -> runner.exportFatJar(List.of(src), target))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot resolve sqrl-cli.jar path");
  }

  @Test
  void given_testClasspath_when_exportFatJar_then_producesJar() throws IOException {
    var runner = JBangRunner.withClasspath(System.getProperty("java.class.path"));
    assumeThat(runner.isJBangAvailable()).isTrue();

    var src = tempDir.resolve("TestUDF.java");
    Files.writeString(
        src,
        """
        import org.apache.flink.table.functions.ScalarFunction;

        public class TestUDF extends ScalarFunction {
          public String eval(String input) {
            return input;
          }
        }
        """);
    var target = tempDir.resolve("TestUDF.jar");

    runner.exportFatJar(List.of(src), target);

    assertThat(target).exists().isRegularFile();
    assertThat(Files.size(target)).isGreaterThan(0);
  }
}
