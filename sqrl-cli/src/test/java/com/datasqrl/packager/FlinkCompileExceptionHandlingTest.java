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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.RootPath;
import com.datasqrl.plan.MainScript;
import com.datasqrl.util.FlinkCompileException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FlinkCompileExceptionHandlingTest {

  @TempDir private Path tempBuildDir;

  @Mock private RootPath rootPath;
  @Mock private PackageJson packageJson;
  @Mock private FilePreprocessingPipeline preprocPipeline;
  @Mock private MainScript mainScript;

  private BuildPath buildPath;
  private Packager packager;

  @BeforeEach
  void setUp() {
    buildPath = new BuildPath(tempBuildDir, null);
    packager = new Packager(rootPath, packageJson, buildPath, preprocPipeline, mainScript);
  }

  @Test
  void givenFlinkCompileException_whenPostprocessError_thenWritesBothDagAndSqlFiles()
      throws Exception {
    // Given
    var sqlStatements =
        Arrays.asList(
            "CREATE TABLE source_table (id INT, name STRING) WITH ('connector' = 'kafka')",
            "INSERT INTO sink_table SELECT * FROM source_table WHERE id > 100");
    var dagString = "DAG Structure:\n  Node1 -> Node2 -> Node3";

    var exception = mock(FlinkCompileException.class);
    when(exception.getFlinkSql()).thenReturn(sqlStatements);
    when(exception.getDag()).thenReturn(dagString);

    // When
    packager.postprocessFlinkCompileError(exception);

    // Then
    var dagFile = tempBuildDir.resolve("compile_error_dag.log");
    var sqlFile = tempBuildDir.resolve("compile_error_sql.log");

    assertThat(dagFile).exists().isRegularFile();
    assertThat(sqlFile).exists().isRegularFile();

    var dagContent = Files.readString(dagFile);
    assertThat(dagContent).isEqualTo(dagString);

    var sqlContent = Files.readString(sqlFile);
    assertThat(sqlContent).isEqualTo(String.join("\n", sqlStatements));
  }

  @Test
  void givenFlinkCompileExceptionWithDagOnly_whenPostprocessError_thenWritesDagFileOnly()
      throws Exception {
    // Given
    var dagString = "DAG Structure:\n  Node1 -> Node2";

    var exception = mock(FlinkCompileException.class);
    when(exception.getFlinkSql()).thenReturn(Collections.emptyList());
    when(exception.getDag()).thenReturn(dagString);

    // When
    packager.postprocessFlinkCompileError(exception);

    // Then
    var dagFile = tempBuildDir.resolve("compile_error_dag.log");
    var sqlFile = tempBuildDir.resolve("compile_error_sql.log");

    assertThat(dagFile).exists().isRegularFile();
    assertThat(sqlFile).doesNotExist();

    var dagContent = Files.readString(dagFile);
    assertThat(dagContent).isEqualTo(dagString);
  }

  @Test
  void givenFlinkCompileExceptionWithSqlOnly_whenPostprocessError_thenWritesSqlFileOnly()
      throws Exception {
    // Given
    var sqlStatements = List.of("SELECT * FROM table1", "SELECT * FROM table2");

    var exception = mock(FlinkCompileException.class);
    when(exception.getFlinkSql()).thenReturn(sqlStatements);
    when(exception.getDag()).thenReturn(null);

    // When
    packager.postprocessFlinkCompileError(exception);

    // Then
    var dagFile = tempBuildDir.resolve("compile_error_dag.log");
    var sqlFile = tempBuildDir.resolve("compile_error_sql.log");

    assertThat(dagFile).doesNotExist();
    assertThat(sqlFile).exists().isRegularFile();

    var sqlContent = Files.readString(sqlFile);
    assertThat(sqlContent).isEqualTo(String.join("\n", sqlStatements));
  }

  @Test
  void givenFlinkCompileExceptionWithEmptyData_whenPostprocessError_thenWritesNoFiles()
      throws Exception {
    // Given
    var exception = mock(FlinkCompileException.class);
    when(exception.getFlinkSql()).thenReturn(null);
    when(exception.getDag()).thenReturn("");

    // When
    packager.postprocessFlinkCompileError(exception);

    // Then
    var dagFile = tempBuildDir.resolve("compile_error_dag.log");
    var sqlFile = tempBuildDir.resolve("compile_error_sql.log");

    assertThat(dagFile).doesNotExist();
    assertThat(sqlFile).doesNotExist();
  }

  @Test
  void givenFlinkCompileExceptionWithBlankDag_whenPostprocessError_thenDoesNotWriteDagFile()
      throws Exception {
    // Given
    var sqlStatements = List.of("SELECT 1");

    var exception = mock(FlinkCompileException.class);
    when(exception.getFlinkSql()).thenReturn(sqlStatements);
    when(exception.getDag()).thenReturn("   "); // Blank string with spaces

    // When
    packager.postprocessFlinkCompileError(exception);

    // Then
    var dagFile = tempBuildDir.resolve("compile_error_dag.log");
    var sqlFile = tempBuildDir.resolve("compile_error_sql.log");

    assertThat(dagFile).doesNotExist();
    assertThat(sqlFile).exists().isRegularFile();
  }

  @Test
  void givenMultipleSqlStatements_whenPostprocessError_thenJoinsWithNewlines() throws Exception {
    // Given
    var sqlStatements =
        Arrays.asList(
            "CREATE TABLE t1 (id INT)",
            "CREATE TABLE t2 (id INT)",
            "INSERT INTO t2 SELECT * FROM t1");

    var exception = mock(FlinkCompileException.class);
    when(exception.getFlinkSql()).thenReturn(sqlStatements);
    when(exception.getDag()).thenReturn(null);

    // When
    packager.postprocessFlinkCompileError(exception);

    // Then
    var sqlFile = tempBuildDir.resolve("compile_error_sql.log");
    assertThat(sqlFile).exists();

    var sqlContent = Files.readString(sqlFile);
    assertThat(sqlContent)
        .isEqualTo(
            "CREATE TABLE t1 (id INT)\n"
                + "CREATE TABLE t2 (id INT)\n"
                + "INSERT INTO t2 SELECT * FROM t1");
  }
}
