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
package com.datasqrl.packager.preprocess;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.JBangRunner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.apache.commons.exec.ExecuteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JBangPreprocessorTest {

  private static final Duration JBANG_JAR_MAX_AGE = Duration.ofHours(2);

  @Mock private JBangRunner jBangRunner;
  @Mock private PackageJson packageJson;
  @Mock private PackageJson.CompilerConfig compilerConfig;
  @Mock private FilePreprocessingPipeline.Context context;

  private JBangPreprocessor underTest;
  private Path libDir;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    libDir = tempDir.resolve("lib");
    Files.createDirectories(libDir);

    when(context.libDir()).thenReturn(libDir);
    when(context.createNewBuildFile(any()))
        .thenAnswer(inv -> tempDir.resolve((Path) inv.getArgument(0)));
    when(jBangRunner.isJBangAvailable()).thenReturn(true);
    when(packageJson.getCompilerConfig()).thenReturn(compilerConfig);
    when(compilerConfig.getJBangJarMaxAge()).thenReturn(JBANG_JAR_MAX_AGE);

    underTest = new JBangPreprocessor(jBangRunner, packageJson);
  }

  @Test
  void given_jbangNotAvailable_when_process_then_skipsProcessing() throws IOException {
    when(jBangRunner.isJBangAvailable()).thenReturn(false);
    var javaFile = createJavaFile("ValidUDF.java", validScalarFunctionContent());

    underTest.process(javaFile, context);
    underTest.complete();

    verifyNoInteractions(context);
    verify(jBangRunner, never()).exportFatJar(any(), any());
  }

  @Test
  void given_nonJavaFile_when_process_then_skipsProcessing() throws IOException {
    var textFile = tempDir.resolve("test.txt");
    Files.writeString(textFile, "some content");

    underTest.process(textFile, context);
    underTest.complete();

    verifyNoInteractions(context);
    verify(jBangRunner, never()).exportFatJar(any(), any());
  }

  @Test
  void given_jbangFileWithoutDepsComment_when_process_then_processesFile() throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        public class TestClass extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("TestClass.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verifyExportedFiles(javaFile);
    verify(context).createNewBuildFile(Path.of("TestClass.function.json"));
  }

  @Test
  void givenJbangJarBuiltWithinConfiguredAge_whenProcess_thenSkipsExport() throws IOException {
    var javaFile = createJavaFile("TestClass.java", validScalarFunctionContent());
    createJbangJar(System.currentTimeMillis() - JBANG_JAR_MAX_AGE.minusMinutes(1).toMillis());

    underTest.process(javaFile, context);
    underTest.complete();

    verify(jBangRunner, never()).exportFatJar(any(), any());
  }

  @Test
  void givenJbangJarBuiltAfterConfiguredAge_whenProcess_thenExportsJar() throws IOException {
    var javaFile = createJavaFile("TestClass.java", validScalarFunctionContent());
    createJbangJar(System.currentTimeMillis() - JBANG_JAR_MAX_AGE.plusMinutes(1).toMillis());

    underTest.process(javaFile, context);
    underTest.complete();

    verifyExportedFiles(javaFile);
  }

  @Test
  void given_validScalarFunctionWithPackage_when_process_then_createsManifestAndExportsJar()
      throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        package com.example.udfs;

        public class MyUDF extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("MyUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verifyExportedFiles(javaFile);
    verify(context).createNewBuildFile(Path.of("MyUDF.function.json"));
  }

  @Test
  void given_validScalarFunctionWithoutPackage_when_process_then_createsManifestAndExportsJar()
      throws IOException {
    var javaFile = createJavaFile("SimpleUDF.java", validScalarFunctionContent());

    underTest.process(javaFile, context);
    underTest.complete();

    verifyExportedFiles(javaFile);
    verify(context).createNewBuildFile(Path.of("SimpleUDF.function.json"));
  }

  @Test
  void given_validTableFunctionWithMultilineDeclaration_when_process_then_createsManifest()
      throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?

        public class MultiLineUDF
            extends TableFunction {
        }
        """;
    var javaFile = createJavaFile("MultiLineUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verifyExportedFiles(javaFile);
    verify(context).createNewBuildFile(Path.of("MultiLineUDF.function.json"));
  }

  @Test
  void given_classNotExtendingFlinkUDF_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        //DEPS some.library:artifact:1.0.0

        public class NotAUDF extends SomeOtherClass {
        }
        """;
    var javaFile = createJavaFile("NotAUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(jBangRunner, never()).exportFatJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_multiplePublicClasses_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        public class FirstUDF extends ScalarFunction {
        }

        public class SecondUDF extends TableFunction {
        }
        """;
    var javaFile = createJavaFile("MultipleClasses.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(jBangRunner, never()).exportFatJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_noPublicClassFound_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        class PrivateClass extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("PrivateClass.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(jBangRunner, never()).exportFatJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_classWithoutExtendsStatement_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        public class NoExtendsClass {
        }
        """;
    var javaFile = createJavaFile("NoExtendsClass.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(jBangRunner, never()).exportFatJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_jbangExportFails_when_process_then_logsWarningButContinues() throws IOException {
    var javaFile = createJavaFile("FailingUDF.java", validScalarFunctionContent());
    doThrow(new ExecuteException("JBang failed", 1)).when(jBangRunner).exportFatJar(any(), any());

    underTest.process(javaFile, context);
    underTest.complete();

    verifyExportedFiles(javaFile);
    verify(context, never()).createNewBuildFile(any());
  }

  @Test
  void given_ioExceptionDuringExport_when_process_then_logsWarningButContinues()
      throws IOException {
    var javaFile = createJavaFile("IOFailUDF.java", validScalarFunctionContent());
    doThrow(new IOException("IO failure")).when(jBangRunner).exportFatJar(any(), any());

    underTest.process(javaFile, context);
    underTest.complete();

    verifyExportedFiles(javaFile);
    verify(context, never()).createNewBuildFile(any());
  }

  @Test
  void given_aggregateFunctionWithSimpleClassName_when_process_then_matchesParentClass()
      throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        public class MyAggregateUDF extends AggregateFunction {
        }
        """;
    var javaFile = createJavaFile("MyAggregateUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verifyExportedFiles(javaFile);
    verify(context).createNewBuildFile(Path.of("MyAggregateUDF.function.json"));
  }

  @Test
  void given_javaFileWithFlinkDeps_when_process_then_accepted() throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        //DEPS org.apache.flink:flink-table-common:2.1.0

        public class MyUDF extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("MyUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verifyExportedFiles(javaFile);
  }

  @Test
  void given_plainJavaFileWithNoUdfClass_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        public class UtilityHelper {
            public static String format(String input) {
                return input.trim();
            }
        }
        """;
    var javaFile = createJavaFile("UtilityHelper.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(jBangRunner, never()).exportFatJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_javaFileWithoutShebang_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        public class MyUDF extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("MyUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(jBangRunner, never()).exportFatJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_multipleJbangFiles_when_complete_then_batchesIntoSingleExportCall()
      throws IOException {
    var content1 =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        public class FirstUDF extends ScalarFunction {
        }
        """;
    var content2 =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        public class SecondUDF extends TableFunction {
        }
        """;
    var file1 = createJavaFile("FirstUDF.java", content1);
    var file2 = createJavaFile("SecondUDF.java", content2);

    underTest.process(file1, context);
    underTest.process(file2, context);
    underTest.complete();

    verifyExportedFiles(file1, file2);
    verify(context).createNewBuildFile(Path.of("FirstUDF.function.json"));
    verify(context).createNewBuildFile(Path.of("SecondUDF.function.json"));
  }

  private Path createJavaFile(String filename, String content) throws IOException {
    var file = tempDir.resolve(filename);
    Files.writeString(file, content);
    return file;
  }

  private void verifyExportedFiles(Path... expectedFiles) throws IOException {
    verify(jBangRunner)
        .exportFatJar(
            argThat(
                jBangFiles ->
                    jBangFiles.stream()
                        .map(JBangPreprocessor.JBangFileInfo::file)
                        .toList()
                        .equals(List.of(expectedFiles))),
            any());
  }

  private void createJbangJar(long buildTime) throws IOException {
    var manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().putValue("Build-Time", String.valueOf(buildTime));
    try (var ignored =
        new JarOutputStream(
            Files.newOutputStream(tempDir.resolve(JBangPreprocessor.JBANG_JAR_NAME)), manifest)) {
      // The manifest is sufficient for JBang JAR validity checks.
    }
  }

  private String validScalarFunctionContent() {
    return """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        public class SimpleUDF extends ScalarFunction {
        }
        """;
  }
}
