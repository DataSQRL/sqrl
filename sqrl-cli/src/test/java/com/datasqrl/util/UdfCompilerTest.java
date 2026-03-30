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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UdfCompilerTest {

  @TempDir private Path tempDir;

  @Test
  void given_runningFromClassesDirectory_when_resolveCliJarPath_then_returnsEmpty() {
    var result = UdfCompiler.resolveCliJarPath();
    assertThat(result).isEmpty();
  }

  @Test
  void given_runningFromClassesDirectory_when_resolveClasspath_then_throwsIllegalState() {
    var compiler = UdfCompiler.create(new MavenDependencyResolver());

    assertThatThrownBy(compiler::resolveClasspath)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot resolve sqrl-cli.jar path");
  }

  @Test
  void given_withClasspath_when_resolveClasspath_then_returnsOverride() {
    var compiler = UdfCompiler.withClasspath("/some/custom.jar", new MavenDependencyResolver());

    assertThat(compiler.resolveClasspath()).isEqualTo("/some/custom.jar");
  }

  @Test
  void given_disabledCompiler_when_compileAndPackage_then_doesNothing() {
    var compiler = UdfCompiler.disabled();
    var src = tempDir.resolve("Dummy.java");
    var target = tempDir.resolve("Dummy.jar");

    assertThatCode(() -> compiler.compileAndPackage(List.of(src), target))
        .doesNotThrowAnyException();
    assertThat(target).doesNotExist();
  }

  @Test
  void given_testClasspath_when_compileAndPackage_then_producesJar() throws IOException {
    var resolver = new MavenDependencyResolver();
    var compiler = UdfCompiler.withClasspath(System.getProperty("java.class.path"), resolver);

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

    compiler.compileAndPackage(List.of(src), target);

    assertThat(target).exists().isRegularFile();
    assertThat(Files.size(target)).isGreaterThan(0);

    try (var jar = new JarFile(target.toFile())) {
      var entryNames = new HashSet<String>();
      jar.entries().asIterator().forEachRemaining(e -> entryNames.add(e.getName()));
      assertThat(entryNames).contains("TestUDF.class");
    }
  }

  @Test
  void given_sourceWithOldJbangShebang_when_compileAndPackage_then_stripsShebangAndCompiles()
      throws IOException {
    var resolver = new MavenDependencyResolver();
    var compiler = UdfCompiler.withClasspath(System.getProperty("java.class.path"), resolver);

    var src = tempDir.resolve("ShebangUDF.java");
    Files.writeString(
        src,
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        import org.apache.flink.table.functions.ScalarFunction;

        public class ShebangUDF extends ScalarFunction {
          public String eval(String input) {
            return input;
          }
        }
        """);
    var target = tempDir.resolve("ShebangUDF.jar");

    compiler.compileAndPackage(List.of(src), target);

    assertThat(target).exists().isRegularFile();
  }

  @Test
  void given_sourceWithSyntaxError_when_compileAndPackage_then_throwsDescriptiveError()
      throws IOException {
    var resolver = new MavenDependencyResolver();
    var compiler = UdfCompiler.withClasspath(System.getProperty("java.class.path"), resolver);

    var src = tempDir.resolve("BrokenUDF.java");
    Files.writeString(
        src,
        """
        import org.apache.flink.table.functions.ScalarFunction;

        public class BrokenUDF extends ScalarFunction {
          public String eval(String input) {
            return input  // missing semicolon
          }
        }
        """);
    var target = tempDir.resolve("BrokenUDF.jar");

    assertThatThrownBy(() -> compiler.compileAndPackage(List.of(src), target))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("UDF compilation failed");
  }

  @Test
  void given_multipleSources_when_compileAndPackage_then_allClassesInJar() throws IOException {
    var resolver = new MavenDependencyResolver();
    var compiler = UdfCompiler.withClasspath(System.getProperty("java.class.path"), resolver);

    var src1 = tempDir.resolve("FirstUDF.java");
    Files.writeString(
        src1,
        """
        import org.apache.flink.table.functions.ScalarFunction;

        public class FirstUDF extends ScalarFunction {
          public String eval(String input) { return input; }
        }
        """);

    var src2 = tempDir.resolve("SecondUDF.java");
    Files.writeString(
        src2,
        """
        import org.apache.flink.table.functions.ScalarFunction;

        public class SecondUDF extends ScalarFunction {
          public Long eval(Long a) { return a; }
        }
        """);

    var target = tempDir.resolve("udfs.jar");

    compiler.compileAndPackage(List.of(src1, src2), target);

    try (var jar = new JarFile(target.toFile())) {
      var entryNames = new HashSet<String>();
      jar.entries().asIterator().forEachRemaining(e -> entryNames.add(e.getName()));
      assertThat(entryNames).contains("FirstUDF.class", "SecondUDF.class");
    }
  }

  @Test
  void given_contentWithShebang_when_stripShebang_then_removesFirstLine() {
    var content = "///usr/bin/env jbang \"$0\" \"$@\" ; exit $?\nimport foo;\npublic class X {}";
    var result = UdfCompiler.stripShebang(content);
    assertThat(result).isEqualTo("import foo;\npublic class X {}");
  }

  @Test
  void given_contentWithoutShebang_when_stripShebang_then_returnsUnchanged() {
    var content = "import foo;\npublic class X {}";
    var result = UdfCompiler.stripShebang(content);
    assertThat(result).isEqualTo(content);
  }
}
