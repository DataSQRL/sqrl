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
package com.datasqrl.loaders;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

class UdfJarClassLoadersTest {

  private static final String RESOURCE = "marker.txt";

  @TempDir Path tempDir;

  private URL firstJar;
  private URL secondJar;

  @BeforeEach
  void setUp() throws IOException {
    firstJar = writeJar(tempDir.resolve("first.jar"));
    secondJar = writeJar(tempDir.resolve("second.jar"));
  }

  @Test
  void givenSameJar_whenForJar_thenReusesLoader() throws IOException {
    try (var loaders = new UdfJarClassLoaders()) {
      assertThat(loaders.forJar(firstJar)).isSameAs(loaders.forJar(firstJar));
      assertThat(loaders.forJar(firstJar)).isNotSameAs(loaders.forJar(secondJar));
    }
  }

  @Test
  void givenOpenLoaders_whenClose_thenJarsAreNoLongerReadable() throws IOException {
    var loaders = new UdfJarClassLoaders();
    var first = loaders.forJar(firstJar);
    var second = loaders.forJar(secondJar);
    assertThat(first.getResource(RESOURCE)).isNotNull();
    assertThat(second.getResource(RESOURCE)).isNotNull();

    loaders.close();

    assertThat(first.getResource(RESOURCE)).isNull();
    assertThat(second.getResource(RESOURCE)).isNull();
  }

  @Test
  void givenSpringManagedBean_whenContextCloses_thenLoadersAreClosed() {
    var ctx = new AnnotationConfigApplicationContext(UdfJarClassLoaders.class);
    var loader = ctx.getBean(UdfJarClassLoaders.class).forJar(firstJar);
    assertThat(loader.getResource(RESOURCE)).isNotNull();

    ctx.close();

    assertThat(loader.getResource(RESOURCE)).isNull();
  }

  private static URL writeJar(Path path) throws IOException {
    try (var jar = new JarOutputStream(Files.newOutputStream(path))) {
      jar.putNextEntry(new JarEntry(RESOURCE));
      jar.write("marker".getBytes());
      jar.closeEntry();
    }
    return path.toUri().toURL();
  }
}
