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

import static com.datasqrl.packager.LambdaUtil.rethrowCall;

import com.datasqrl.loaders.ClasspathFunctionLoader;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

/*
 * Reads a jar and creates sqrl manifest entries in the build directory
 */
@Slf4j
public class JarPreprocessor implements Preprocessor {

  private static final ObjectMapper MAPPER = SqrlObjectMapper.INSTANCE;
  private static final String SERVICES_PATH = "META-INF/services/";
  private static final Set<String> FLINK_UDFS =
      ClasspathFunctionLoader.FLINK_UDF_CLASSES.stream()
          .map(Class::getCanonicalName)
          .collect(Collectors.toSet());

  @SneakyThrows
  @Override
  public void process(Path path, FilePreprocessingPipeline.Context context) {
    if (!path.getFileName().toString().endsWith(".jar")) return;
    try (var file = new java.util.jar.JarFile(path.toFile())) {
      file.stream()
          .filter(this::isValidEntry)
          .filter(entry -> FLINK_UDFS.contains(getClassName(entry)))
          .forEach(entry -> rethrowCall(() -> processJarEntry(entry, file, context, path)));
      context.copyToLib(path);
    } catch (Exception e) {
      log.warn("Could not read jar in path:" + path, e);
    }
  }

  /** Gets the class name for the jar entry */
  private String getClassName(java.util.jar.JarEntry entry) {
    return entry.getName().substring(entry.getName().lastIndexOf("/") + 1);
  }

  /** Processes a single jar entry */
  private Void processJarEntry(
      JarEntry entry, JarFile file, FilePreprocessingPipeline.Context processorContext, Path path)
      throws IOException {
    var input = file.getInputStream(entry);
    var classes = IOUtils.readLines(input, Charset.defaultCharset());

    for (var clazz : classes) {
      var obj = MAPPER.createObjectNode();
      obj.put("language", "java");
      obj.put("functionClass", clazz);
      obj.put("jarPath", path.toFile().getName());

      // Create a file in a temporary directory
      var functionName = clazz.substring(clazz.lastIndexOf('.') + 1);
      var functionPath =
          processorContext.createNewBuildFile(Path.of(functionName + ".function.json"));
      MAPPER.writeValue(functionPath.toFile(), obj);
    }

    return null;
  }

  /** Checks if the jar entry is valid */
  private boolean isValidEntry(java.util.jar.JarEntry entry) {
    return entry.getName().startsWith(SERVICES_PATH) && !entry.getName().endsWith("/");
  }
}
