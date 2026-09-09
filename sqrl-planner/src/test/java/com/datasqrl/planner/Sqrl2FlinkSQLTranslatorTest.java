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
package com.datasqrl.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

@EnabledOnOs(OS.LINUX)
class Sqrl2FlinkSQLTranslatorTest {

  @TempDir Path workspace;

  @Test
  void givenUdfJarOpenedDuringCompile_whenClose_thenJarHandleIsReleased() throws IOException {
    var workspacePaths = new WorkspacePaths(workspace, workspace, workspace, workspace);
    var jar = writeJar(workspacePaths.getUdfPath().resolve("udf.jar"));
    var flink = mock(FlinkStreamEngine.class);
    when(flink.getExecutionMode()).thenReturn(RuntimeExecutionMode.STREAMING);
    when(flink.getBaseConfiguration()).thenReturn(new Configuration());
    when(flink.getStreamingSpecificConfig()).thenReturn(new Configuration());
    var compilerConfig = mock(CompilerConfig.class);
    when(compilerConfig.predicatePushdownRules()).thenReturn(PredicatePushdownRules.DEFAULT);

    var translator = new Sqrl2FlinkSQLTranslator(workspacePaths, flink, compilerConfig);
    assertThatThrownBy(() -> translator.addUserDefinedFunction("missing", "no.such.Udf", true))
        .isInstanceOf(Exception.class);
    assertThat(openFiles()).contains(jar);

    translator.close();

    assertThat(openFiles()).doesNotContain(jar);
  }

  private static Path writeJar(Path path) throws IOException {
    Files.createDirectories(path.getParent());
    try (var jar = new JarOutputStream(Files.newOutputStream(path))) {
      jar.putNextEntry(new JarEntry("marker.txt"));
      jar.write("marker".getBytes());
      jar.closeEntry();
    }
    return path.toRealPath();
  }

  private static Set<Path> openFiles() throws IOException {
    var open = new HashSet<Path>();
    try (var fds = Files.list(Path.of("/proc/self/fd"))) {
      for (var fd : fds.toList()) {
        try {
          open.add(Files.readSymbolicLink(fd));
        } catch (IOException ignored) {
        }
      }
    }
    return open;
  }
}
