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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.JBangRunner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

  @Mock private JBangRunner jBangRunner;
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

    underTest = new JBangPreprocessor(jBangRunner);
  }

  @Test
  void given_jbangNotAvailable_when_process_then_skipsProcessing() throws IOException {
    // given
    when(jBangRunner.isJBangAvailable()).thenReturn(false);
    var javaFile = createJavaFile("ValidUDF.java", validScalarFunctionContent());

    // when
    underTest.process(javaFile, context);

    // then
    verifyNoInteractions(context);
    verify(jBangRunner, never()).exportLocalJar(any(), any());
  }

  @Test
  void given_nonJavaFile_when_process_then_skipsProcessing() throws IOException {
    // given
    var textFile = tempDir.resolve("test.txt");
    Files.writeString(textFile, "some content");

    // when
    underTest.process(textFile, context);

    // then
    verifyNoInteractions(context);
    verify(jBangRunner, never()).exportLocalJar(any(), any());
  }

  @Test
  void given_javaFileWithoutDepsComment_when_process_then_skipsProcessing() throws IOException {
    // given
    var content =
        """
        public class TestClass extends ScalarFunction {
            // No //DEPS comment
        }
        """;
    var javaFile = createJavaFile("TestClass.java", content);

    // when
    underTest.process(javaFile, context);

    // then
    verifyNoInteractions(context);
    verify(jBangRunner, never()).exportLocalJar(any(), any());
  }

  @Test
  void given_validScalarFunctionWithPackage_when_process_then_createsManifestAndExportsJar()
      throws IOException {
    // given
    var content =
        """
        //DEPS io.flink:flink-table-api-java:1.17.0
        package com.example.udfs;

        public class MyUDF extends ScalarFunction {
            // implementation
        }
        """;
    var javaFile = createJavaFile("MyUDF.java", content);

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner).exportLocalJar(eq(javaFile), any());
    verify(context).createNewBuildFile(Path.of("MyUDF.function.json"));
  }

  @Test
  void given_validScalarFunctionWithoutPackage_when_process_then_createsManifestAndExportsJar()
      throws IOException {
    // given
    var javaFile = createJavaFile("SimpleUDF.java", validScalarFunctionContent());

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner).exportLocalJar(eq(javaFile), any());
    verify(context).createNewBuildFile(Path.of("SimpleUDF.function.json"));
  }

  @Test
  void given_validTableFunctionWithMultilineDeclaration_when_process_then_createsManifest()
      throws IOException {
    // given
    var content =
        """
        //DEPS io.flink:flink-table-api-java:1.17.0

        public class MultiLineUDF
            extends TableFunction {
            // implementation
        }
        """;
    var javaFile = createJavaFile("MultiLineUDF.java", content);

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner).exportLocalJar(eq(javaFile), any());
    verify(context).createNewBuildFile(Path.of("MultiLineUDF.function.json"));
  }

  @Test
  void given_classNotExtendingFlinkUDF_when_process_then_skipsProcessing() throws IOException {
    // given
    var content =
        """
        //DEPS some.library:artifact:1.0.0

        public class NotAUDF extends SomeOtherClass {
            // implementation
        }
        """;
    var javaFile = createJavaFile("NotAUDF.java", content);

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner, never()).exportLocalJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_multiplePublicClasses_when_process_then_skipsProcessing() throws IOException {
    // given
    var content =
        """
        //DEPS io.flink:flink-table-api-java:1.17.0

        public class FirstUDF extends ScalarFunction {
            // implementation
        }

        public class SecondUDF extends TableFunction {
            // implementation
        }
        """;
    var javaFile = createJavaFile("MultipleClasses.java", content);

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner, never()).exportLocalJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_noPublicClassFound_when_process_then_skipsProcessing() throws IOException {
    // given
    var content =
        """
        //DEPS io.flink:flink-table-api-java:1.17.0

        class PrivateClass extends ScalarFunction {
            // implementation
        }
        """;
    var javaFile = createJavaFile("PrivateClass.java", content);

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner, never()).exportLocalJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_classWithoutExtendsStatement_when_process_then_skipsProcessing() throws IOException {
    // given
    var content =
        """
        //DEPS io.flink:flink-table-api-java:1.17.0

        public class NoExtendsClass {
            // implementation
        }
        """;
    var javaFile = createJavaFile("NoExtendsClass.java", content);

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner, never()).exportLocalJar(any(), any());
    verifyNoInteractions(context);
  }

  @Test
  void given_jbangExportFails_when_process_then_logsWarningButContinues() throws IOException {
    // given
    var javaFile = createJavaFile("FailingUDF.java", validScalarFunctionContent());
    doThrow(new ExecuteException("JBang failed", 1)).when(jBangRunner).exportLocalJar(any(), any());

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner).exportLocalJar(eq(javaFile), any());
    verify(context, never()).createNewBuildFile(any());
  }

  @Test
  void given_ioExceptionDuringExport_when_process_then_logsWarningButContinues()
      throws IOException {
    // given
    var javaFile = createJavaFile("IOFailUDF.java", validScalarFunctionContent());
    doThrow(new IOException("IO failure")).when(jBangRunner).exportLocalJar(any(), any());

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner).exportLocalJar(eq(javaFile), any());
    verify(context, never()).createNewBuildFile(any());
  }

  @Test
  void given_aggregateFunctionWithSimpleClassName_when_process_then_matchesParentClass()
      throws IOException {
    // given
    var content =
        """
        //DEPS io.flink:flink-table-api-java:1.17.0

        public class MyAggregateUDF extends AggregateFunction {
            // implementation
        }
        """;
    var javaFile = createJavaFile("MyAggregateUDF.java", content);

    // when
    underTest.process(javaFile, context);

    // then
    verify(jBangRunner).exportLocalJar(eq(javaFile), any());
    verify(context).createNewBuildFile(Path.of("MyAggregateUDF.function.json"));
  }

  private Path createJavaFile(String filename, String content) throws IOException {
    var file = tempDir.resolve(filename);
    Files.writeString(file, content);
    return file;
  }

  private String validScalarFunctionContent() {
    return """
        //DEPS io.flink:flink-table-api-java:1.17.0

        public class SimpleUDF extends ScalarFunction {
            // implementation
        }
        """;
  }
}
