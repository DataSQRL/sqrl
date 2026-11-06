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
package com.datasqrl.packager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.IncludeConfig;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.packager.preprocess.JBangPreprocessor;
import com.datasqrl.packager.preprocess.JBangPreprocessor.JBangFileInfo;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.plan.MainScript;
import com.datasqrl.util.JBangRunner;
import com.datasqrl.util.SqrlObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Preprocesses projects with JBang UDFs, where only the JBang CLI is stubbed. */
@ExtendWith(MockitoExtension.class)
class PackagerTest {

  @TempDir private Path tempDir;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private PackageJson packageJson;

  @Mock private JBangRunner jBangRunner;
  @Mock private MainScript mainScript;

  private Path projectDir;
  private WorkspacePaths workspacePaths;
  private ErrorCollector errors;
  private Packager underTest;

  @BeforeEach
  void setUp() throws IOException {
    projectDir = tempDir.resolve("project");
    var buildDir = projectDir.resolve("build");
    workspacePaths = new WorkspacePaths(tempDir, buildDir, buildDir.resolve("deploy"), projectDir);
    errors = ErrorCollector.root();

    var preprocessors =
        Set.<Preprocessor>of(new JarPreprocessor(), new JBangPreprocessor(jBangRunner, errors));
    var pipeline = new FilePreprocessingPipeline(workspacePaths, buildDir, preprocessors);
    underTest = new Packager(workspacePaths, packageJson, pipeline, mainScript);

    when(jBangRunner.isJBangAvailable()).thenReturn(true);
    doAnswer(invocation -> exportFatJar(invocation.getArgument(0), invocation.getArgument(1)))
        .when(jBangRunner)
        .exportFatJar(any(), any());
  }

  @Test
  void givenJbangUdfsInTwoDirectories_whenPreprocessedTwice_thenEachManifestIsNextToItsUdf()
      throws IOException {
    givenIncludes();
    writeUdf(projectDir.resolve("a/usrlib/x"), "Foo", 1);
    writeUdf(projectDir.resolve("b/usrlib/y"), "Bar", 1);

    for (var i = 0; i < 2; i++) {
      underTest.preprocess(errors);

      assertThat(manifests())
          .containsOnly(
              entry("a/usrlib/x/Foo.function.json", "jbang-udfs.jar"),
              entry("b/usrlib/y/Bar.function.json", "jbang-udfs.jar"));
    }

    // The second preprocessing reuses the JAR cached in the deepest common directory
    verify(jBangRunner).exportFatJar(any(), any());
    assertThat(projectDir.resolve("jbang-udfs.jar")).isRegularFile();
    assertThat(warnings()).isEmpty();
  }

  @Test
  void givenJbangUdfsInProjectAndIncludeOutsideOfIt_whenPreprocessedTwice_thenEachHasItsOwnJar()
      throws IOException {
    givenIncludes(include("core", "../core/package.json"));
    writeUdf(tempDir.resolve("core/usrlib/s"), "Baz", 1);
    writeUdf(projectDir.resolve("usrlib/m"), "Foo", 1);

    for (var i = 0; i < 2; i++) {
      underTest.preprocess(errors);

      assertThat(manifests())
          .containsOnly(
              entry("core/usrlib/s/Baz.function.json", "jbang-udfs-core.jar"),
              entry("usrlib/m/Foo.function.json", "jbang-udfs.jar"));
    }

    verify(jBangRunner, times(2)).exportFatJar(any(), any());
    assertThat(warnings())
        .containsOnly(
            "Several projects bundle JBang UDFs into separate JARs: jbang-udfs.jar,"
                + " jbang-udfs-core.jar. If these JARs ship different versions of the same"
                + " dependency, it can cause failures at runtime.");
  }

  @Test
  void givenJbangUdfsInTwoIncludesOutsideOfProject_whenPreprocess_thenWarnsAboutSeparateJars()
      throws IOException {
    givenIncludes(
        include("core", "../core/package.json"), include("sales", "../sales/package.json"));
    writeUdf(tempDir.resolve("core"), "Bar", 1);
    writeUdf(tempDir.resolve("sales"), "Baz", 1);

    underTest.preprocess(errors);

    assertThat(warnings())
        .singleElement()
        .asString()
        .contains("separate JARs: jbang-udfs-core.jar, jbang-udfs-sales.jar.");
  }

  @Test
  void givenJbangUdfOfIncludeInsideOfProjectFolder_whenPreprocess_thenNeitherWarnsNorFails()
      throws IOException {
    givenIncludes(include("core", "modules/core/package.json"));
    writeUdf(projectDir.resolve("modules/core"), "Foo", 1);

    underTest.preprocess(errors);

    // The project bundles the UDF of the included project, as its folder contains the script
    assertThat(manifests())
        .containsOnly(
            entry("core/Foo.function.json", "jbang-udfs-core.jar"),
            entry("modules/core/Foo.function.json", "jbang-udfs.jar"));
    assertThat(warnings()).isEmpty();
    assertThat(errors.hasErrors()).isFalse();
  }

  @Test
  void givenDifferentJbangUdfsOfSameClassInProjectAndInclude_whenPreprocess_thenFails()
      throws IOException {
    givenIncludes(include("core", "../core/package.json"));
    writeUdf(tempDir.resolve("core"), "Foo", 1);
    writeUdf(projectDir, "Foo", 2);

    assertThatThrownBy(() -> underTest.preprocess(errors))
        .hasMessageContaining(
            "Several projects define different JBang UDFs with the same class name:"
                + " Foo (jbang-udfs.jar, jbang-udfs-core.jar).");
  }

  @Test
  void givenCachedJbangJarAndNoJbang_whenPreprocess_thenRegistersCachedJar() throws IOException {
    givenIncludes();
    writeUdf(projectDir.resolve("usrlib"), "Foo", 1);
    underTest.preprocess(errors);

    when(jBangRunner.isJBangAvailable()).thenReturn(false);
    underTest.preprocess(errors);

    assertThat(manifests()).containsOnly(entry("usrlib/Foo.function.json", "jbang-udfs.jar"));
    verify(jBangRunner).exportFatJar(any(), any());
  }

  private void givenIncludes(IncludeConfig... includes) {
    when(packageJson.getScriptConfig().getIncludeConfigs()).thenReturn(List.of(includes));
  }

  private static IncludeConfig include(String namespace, String packageFile) {
    var include = mock(IncludeConfig.class);
    when(include.getNamespace()).thenReturn(namespace);
    when(include.getPackage()).thenReturn(packageFile);
    return include;
  }

  private static void writeUdf(Path dir, String className, int result) throws IOException {
    Files.createDirectories(dir);
    Files.writeString(
        dir.resolve(className + ".java"),
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        public class %s extends ScalarFunction {
          public Integer eval() {
            return %d;
          }
        }
        """
            .formatted(className, result));
  }

  /** Stands in for the JBang CLI, which would add the compiled UDFs and their dependencies. */
  private static Void exportFatJar(Deque<JBangFileInfo> jBangFiles, Path fatJar)
      throws IOException {
    Files.createDirectories(fatJar.getParent());
    try (var ignored = new JarOutputStream(Files.newOutputStream(fatJar))) {
      // An empty JAR, that is rebuilt below as after a JBang export
    }
    JBangRunner.create().rebuildFatJar(fatJar, List.copyOf(jBangFiles));
    return null;
  }

  /** Maps the path of each function manifest in the build directory to its JAR. */
  private Map<String, String> manifests() throws IOException {
    var buildDir = workspacePaths.buildDir();
    try (var files = Files.walk(buildDir)) {
      return files
          .filter(file -> file.toString().endsWith(".function.json"))
          .collect(
              Collectors.toMap(
                  file -> buildDir.relativize(file).toString(), PackagerTest::jarPath));
    }
  }

  @SneakyThrows
  private static String jarPath(Path manifest) {
    return SqrlObjectMapper.INSTANCE.readTree(manifest.toFile()).get("jarPath").asText();
  }

  private List<String> warnings() {
    return StreamSupport.stream(errors.spliterator(), false)
        .filter(ErrorMessage::isWarning)
        .map(ErrorMessage::getMessage)
        .toList();
  }
}
