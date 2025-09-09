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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JBangRunnerTest {

  @TempDir private Path tempDir;

  @Test
  void
      given_jarWithClassPathInManifest_when_removeClassPathFromJarManifest_then_removesClassPathEntry()
          throws IOException {
    // given
    var jarPath = createJarWithManifest(true);
    var runner = JBangRunner.create();

    // when
    runner.removeClassPathFromJarManifest(jarPath);

    // then
    try (var jarFile = new JarFile(jarPath.toFile())) {
      var manifest = jarFile.getManifest();
      assertThat(manifest).isNotNull();
      assertThat(manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH)).isNull();
      assertThat(manifest.getMainAttributes().getValue(Attributes.Name.MAIN_CLASS))
          .isEqualTo("com.example.Main");
    }
  }

  @Test
  void
      given_jarWithoutClassPathInManifest_when_removeClassPathFromJarManifest_then_manifestRemainsUnchanged()
          throws IOException {
    // given
    var jarPath = createJarWithManifest(false);
    var runner = JBangRunner.create();

    // when
    runner.removeClassPathFromJarManifest(jarPath);

    // then
    try (var jarFile = new JarFile(jarPath.toFile())) {
      var manifest = jarFile.getManifest();
      assertThat(manifest).isNotNull();
      assertThat(manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH)).isNull();
      assertThat(manifest.getMainAttributes().getValue(Attributes.Name.MAIN_CLASS))
          .isEqualTo("com.example.Main");
    }
  }

  @Test
  void given_jarWithNoManifest_when_removeClassPathFromJarManifest_then_doesNothing()
      throws IOException {
    // given
    var jarPath = createJarWithoutManifest();
    var runner = JBangRunner.create();

    // when
    runner.removeClassPathFromJarManifest(jarPath);

    // then
    try (var jarFile = new JarFile(jarPath.toFile())) {
      assertThat(jarFile.getManifest()).isNull();
    }
  }

  @Test
  void
      given_jarWithMultipleEntries_when_removeClassPathFromJarManifest_then_preservesAllOtherEntries()
          throws IOException {
    // given
    var jarPath = createJarWithMultipleEntries();
    var runner = JBangRunner.create();

    // when
    runner.removeClassPathFromJarManifest(jarPath);

    // then
    try (var jarFile = new JarFile(jarPath.toFile())) {
      var manifest = jarFile.getManifest();
      assertThat(manifest).isNotNull();
      assertThat(manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH)).isNull();

      // Verify all other entries are preserved
      var entryNames = jarFile.stream().map(JarEntry::getName).toList();
      assertThat(entryNames)
          .containsExactlyInAnyOrder(
              "META-INF/MANIFEST.MF",
              "com/example/Test.class",
              "config.properties",
              "nested/file.txt");

      // Verify entry content is preserved
      var testClassEntry = jarFile.getJarEntry("com/example/Test.class");
      try (var inputStream = jarFile.getInputStream(testClassEntry)) {
        var content = new String(inputStream.readAllBytes());
        assertThat(content).isEqualTo("test class content");
      }
    }
  }

  @Test
  void given_nonExistentJar_when_removeClassPathFromJarManifest_then_handlesGracefully() {
    // given
    var nonExistentJar = tempDir.resolve("non-existent.jar");
    var runner = JBangRunner.create();

    // when/then - should not throw exception
    runner.removeClassPathFromJarManifest(nonExistentJar);
  }

  private Path createJarWithManifest(boolean includeClassPath) throws IOException {
    var jarPath = tempDir.resolve("test.jar");
    var manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "com.example.Main");

    if (includeClassPath) {
      manifest
          .getMainAttributes()
          .put(Attributes.Name.CLASS_PATH, "lib/dependency1.jar lib/dependency2.jar");
    }

    try (var output = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
      // Add a dummy entry
      output.putNextEntry(new JarEntry("com/example/Test.class"));
      output.write("test content".getBytes());
      output.closeEntry();
    }

    return jarPath;
  }

  private Path createJarWithoutManifest() throws IOException {
    var jarPath = tempDir.resolve("no-manifest.jar");

    try (var output = new JarOutputStream(Files.newOutputStream(jarPath))) {
      output.putNextEntry(new JarEntry("com/example/Test.class"));
      output.write("test content".getBytes());
      output.closeEntry();
    }

    return jarPath;
  }

  private Path createJarWithMultipleEntries() throws IOException {
    var jarPath = tempDir.resolve("multiple-entries.jar");
    var manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "com.example.Main");
    manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, "lib/dep.jar");

    try (var output = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
      // Add multiple entries
      output.putNextEntry(new JarEntry("com/example/Test.class"));
      output.write("test class content".getBytes());
      output.closeEntry();

      output.putNextEntry(new JarEntry("config.properties"));
      output.write("property=value".getBytes());
      output.closeEntry();

      output.putNextEntry(new JarEntry("nested/file.txt"));
      output.write("nested content".getBytes());
      output.closeEntry();
    }

    return jarPath;
  }
}
