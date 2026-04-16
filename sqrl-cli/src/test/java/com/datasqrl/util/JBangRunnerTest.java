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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JBangRunnerTest {

  @TempDir private Path tempDir;

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
  void given_fatJarWithSignatureFiles_when_cleanFatJar_then_signaturesRemoved() throws IOException {
    var fatJar = tempDir.resolve("fat.jar");
    try (var out = new JarOutputStream(Files.newOutputStream(fatJar))) {
      out.putNextEntry(new JarEntry("META-INF/MANIFEST.MF"));
      out.write("Manifest-Version: 1.0\n".getBytes());
      out.closeEntry();
      out.putNextEntry(new JarEntry("META-INF/SIG-VENDOR.SF"));
      out.write(new byte[] {1});
      out.closeEntry();
      out.putNextEntry(new JarEntry("META-INF/SIG-VENDOR.DSA"));
      out.write(new byte[] {2});
      out.closeEntry();
      out.putNextEntry(new JarEntry("META-INF/SIG-VENDOR.RSA"));
      out.write(new byte[] {3});
      out.closeEntry();
      out.putNextEntry(new JarEntry("META-INF/services/some.Service"));
      out.write("com.example.Impl\n".getBytes());
      out.closeEntry();
      out.putNextEntry(new JarEntry("MyUDF.class"));
      out.write(new byte[] {10, 20, 30});
      out.closeEntry();
    }

    JBangRunner.create().cleanFatJar(fatJar);

    try (var jar = new JarFile(fatJar.toFile())) {
      var entryNames = new HashSet<String>();
      jar.entries().asIterator().forEachRemaining(e -> entryNames.add(e.getName()));

      assertThat(entryNames)
          .contains("META-INF/MANIFEST.MF", "META-INF/services/some.Service", "MyUDF.class");
      assertThat(entryNames)
          .doesNotContain(
              "META-INF/SIG-VENDOR.SF", "META-INF/SIG-VENDOR.DSA", "META-INF/SIG-VENDOR.RSA");
    }
  }

  @Test
  void given_fatJarWithDirectoryEntry_when_cleanFatJar_then_directoryDropped() throws IOException {
    var fatJar = tempDir.resolve("fat.jar");
    try (var out = new JarOutputStream(Files.newOutputStream(fatJar))) {
      out.putNextEntry(new JarEntry("com/"));
      out.closeEntry();
      out.putNextEntry(new JarEntry("MyUDF.class"));
      out.write(new byte[] {10, 20, 30});
      out.closeEntry();
    }

    JBangRunner.create().cleanFatJar(fatJar);

    try (var jar = new JarFile(fatJar.toFile())) {
      var entryNames = new HashSet<String>();
      jar.entries().asIterator().forEachRemaining(e -> entryNames.add(e.getName()));

      assertThat(entryNames).contains("MyUDF.class").doesNotContain("com/");
    }
  }
}
