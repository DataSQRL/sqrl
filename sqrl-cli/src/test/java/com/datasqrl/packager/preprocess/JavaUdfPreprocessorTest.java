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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.MavenDependencyResolver;
import com.datasqrl.util.UdfCompiler;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
class JavaUdfPreprocessorTest {

  @Mock private UdfCompiler udfCompiler;
  @Mock private FilePreprocessingPipeline.Context context;

  private MavenDependencyResolver dependencyResolver;
  private JavaUdfPreprocessor underTest;
  private Path libDir;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    libDir = tempDir.resolve("lib");
    Files.createDirectories(libDir);

    dependencyResolver = new MavenDependencyResolver();

    when(context.libDir()).thenReturn(libDir);
    when(context.createNewBuildFile(any()))
        .thenAnswer(inv -> tempDir.resolve((Path) inv.getArgument(0)));

    underTest = new JavaUdfPreprocessor(udfCompiler, dependencyResolver);
  }

  @Test
  void given_nonJavaFile_when_process_then_skipsProcessing() throws IOException {
    var textFile = tempDir.resolve("test.txt");
    Files.writeString(textFile, "some content");

    underTest.process(textFile, context);
    underTest.complete();

    verifyNoInteractions(context);
    verify(udfCompiler, never()).compileAndPackage(any(), any());
  }

  @Test
  void given_plainJavaFileExtendingScalarFunction_when_process_then_processesFile()
      throws IOException {
    var content =
        """
        public class TestClass extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("TestClass.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler).compileAndPackage(eq(List.of(javaFile)), any());
    verify(context).createNewBuildFile(Path.of("TestClass.function.json"));
  }

  @Test
  void given_javaFileWithOldJbangShebang_when_process_then_stillProcessesFile() throws IOException {
    var content =
        """
        ///usr/bin/env jbang "$0" "$@" ; exit $?
        public class TestClass extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("TestClass.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler).compileAndPackage(eq(List.of(javaFile)), any());
    verify(context).createNewBuildFile(Path.of("TestClass.function.json"));
  }

  @Test
  void given_validScalarFunctionWithPackage_when_process_then_createsManifest() throws IOException {
    var content =
        """
        package com.example.udfs;

        public class MyUDF extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("MyUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler).compileAndPackage(eq(List.of(javaFile)), any());
    verify(context).createNewBuildFile(Path.of("MyUDF.function.json"));
  }

  @Test
  void given_validTableFunctionWithMultilineDeclaration_when_process_then_createsManifest()
      throws IOException {
    var content =
        """
        public class MultiLineUDF
            extends TableFunction {
        }
        """;
    var javaFile = createJavaFile("MultiLineUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler).compileAndPackage(eq(List.of(javaFile)), any());
    verify(context).createNewBuildFile(Path.of("MultiLineUDF.function.json"));
  }

  @Test
  void given_classNotExtendingFlinkUDF_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        public class NotAUDF extends SomeOtherClass {
        }
        """;
    var javaFile = createJavaFile("NotAUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler, never()).compileAndPackage(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_multiplePublicClasses_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        public class FirstUDF extends ScalarFunction {
        }

        public class SecondUDF extends TableFunction {
        }
        """;
    var javaFile = createJavaFile("MultipleClasses.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler, never()).compileAndPackage(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_noPublicClassFound_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        class PrivateClass extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("PrivateClass.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler, never()).compileAndPackage(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_classWithoutExtendsStatement_when_process_then_skipsProcessing() throws IOException {
    var content =
        """
        public class NoExtendsClass {
        }
        """;
    var javaFile = createJavaFile("NoExtendsClass.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler, never()).compileAndPackage(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_compilationFails_when_complete_then_logsWarningButContinues() throws IOException {
    var javaFile = createJavaFile("FailingUDF.java", validScalarFunctionContent());
    doThrow(new IOException("Compilation failed"))
        .when(udfCompiler)
        .compileAndPackage(any(), any());

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler).compileAndPackage(eq(List.of(javaFile)), any());
    verify(context, never()).createNewBuildFile(any());
  }

  @Test
  void given_javaFileWithFlinkJdeps_when_process_then_throwsError() throws IOException {
    var content =
        """
        //JDEPS org.apache.flink:flink-table-common:2.2.0

        public class MyUDF extends ScalarFunction {
        }
        """;
    var javaFile = createJavaFile("MyUDF.java", content);

    assertThatThrownBy(() -> underTest.process(javaFile, context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Flink dependencies are provided automatically via classpath");
  }

  @Test
  void given_aggregateFunctionWithSimpleClassName_when_process_then_matchesParentClass()
      throws IOException {
    var content =
        """
        public class MyAggregateUDF extends AggregateFunction {
        }
        """;
    var javaFile = createJavaFile("MyAggregateUDF.java", content);

    underTest.process(javaFile, context);
    underTest.complete();

    verify(udfCompiler).compileAndPackage(eq(List.of(javaFile)), any());
    verify(context).createNewBuildFile(Path.of("MyAggregateUDF.function.json"));
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

    verify(udfCompiler, never()).compileAndPackage(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_multipleJavaUdfFiles_when_complete_then_batchesIntoSingleCompileCall()
      throws IOException {
    var content1 =
        """
        public class FirstUDF extends ScalarFunction {
        }
        """;
    var content2 =
        """
        public class SecondUDF extends TableFunction {
        }
        """;
    var file1 = createJavaFile("FirstUDF.java", content1);
    var file2 = createJavaFile("SecondUDF.java", content2);

    underTest.process(file1, context);
    underTest.process(file2, context);
    underTest.complete();

    verify(udfCompiler).compileAndPackage(eq(List.of(file1, file2)), any());
    verify(context).createNewBuildFile(Path.of("FirstUDF.function.json"));
    verify(context).createNewBuildFile(Path.of("SecondUDF.function.json"));
  }

  private Path createJavaFile(String filename, String content) throws IOException {
    var file = tempDir.resolve(filename);
    Files.writeString(file, content);
    return file;
  }

  private String validScalarFunctionContent() {
    return """
        public class SimpleUDF extends ScalarFunction {
        }
        """;
  }
}
