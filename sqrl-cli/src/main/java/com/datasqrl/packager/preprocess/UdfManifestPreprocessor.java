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

import com.datasqrl.loaders.ClasspathFunctionLoader;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

/**
 * Abstract base class for preprocessors that extract User-Defined Function (UDF) manifests from JAR
 * files. Scans JAR files for META-INF/services entries that correspond to Flink UDF classes and
 * creates SQRL function manifest files (.function.json) containing metadata about Java functions
 * callable from SQRL scripts.
 */
@Slf4j
abstract class UdfManifestPreprocessor implements Preprocessor {

  private static final ObjectMapper MAPPER = SqrlObjectMapper.INSTANCE;
  private static final String SERVICES_PATH = "META-INF/services/";
  private static final Set<String> FLINK_UDFS =
      ClasspathFunctionLoader.FLINK_UDF_CLASSES.stream()
          .map(Class::getCanonicalName)
          .collect(Collectors.toSet());

  void extractSqrlManifests(Path jarPath, FilePreprocessingPipeline.Context ctx) {
    try (var file = new JarFile(jarPath.toFile())) {

      file.stream()
          .filter(UdfManifestPreprocessor::isFlinkUdf)
          .forEach(entry -> extractManifestsFromEntry(entry, file, jarPath, ctx));

    } catch (Exception e) {
      log.warn("Could not read JAR file:" + jarPath, e);
    }
  }

  @SneakyThrows
  private void extractManifestsFromEntry(
      JarEntry entry, JarFile file, Path jarPath, FilePreprocessingPipeline.Context ctx) {
    var jarEntryStream = file.getInputStream(entry);
    var classes = IOUtils.readLines(jarEntryStream, Charset.defaultCharset());

    for (var cls : classes) {
      var obj = MAPPER.createObjectNode();
      obj.put("language", "java");
      obj.put("functionClass", cls);
      obj.put("jarPath", jarPath.toFile().getName());

      var fnName = cls.substring(cls.lastIndexOf('.') + 1);
      var fnPath = ctx.createNewBuildFile(Path.of(fnName + ".function.json"));

      MAPPER.writeValue(fnPath.toFile(), obj);
    }
  }

  private static boolean isFlinkUdf(JarEntry entry) {
    return FLINK_UDFS.contains(getClassName(entry)) && isAddedToServices(entry);
  }

  private static String getClassName(JarEntry entry) {
    return entry.getName().substring(entry.getName().lastIndexOf("/") + 1);
  }

  private static boolean isAddedToServices(JarEntry entry) {
    return entry.getName().startsWith(SERVICES_PATH) && !entry.getName().endsWith("/");
  }
}
