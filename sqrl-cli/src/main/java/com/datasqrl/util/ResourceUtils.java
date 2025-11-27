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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;

/** Utility class to help work with resource files. */
public class ResourceUtils {
  /**
   * Lists all files in a resource directory, works both in IDE and JAR.
   *
   * @param resourcePath Path relative to resources root (e.g., "templates/init")
   * @return List of resource paths
   */
  @SneakyThrows
  public static List<String> listResourceFiles(String resourcePath) {
    var classLoader = ResourceUtils.class.getClassLoader();
    var resource = classLoader.getResource(resourcePath);

    if (resource == null) {
      throw new IOException("Resource path not found: " + resourcePath);
    }

    var uri = toURI(resource);

    if (uri.getScheme().equals("jar")) {
      return listFilesInJar(uri, resourcePath);
    } else {
      return listFilesInFileSystem(Paths.get(uri), resourcePath);
    }
  }

  /** Reads a resource file as InputStream. */
  public static InputStream getResourceAsStream(String resourcePath) {
    return ResourceUtils.class.getClassLoader().getResourceAsStream(resourcePath);
  }

  @SneakyThrows
  public static Properties loadProperties(String resourcePath) {
    var props = new Properties();

    try (var is = getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IllegalArgumentException("Missing template config: " + resourcePath);
      }
      props.load(new InputStreamReader(is, StandardCharsets.UTF_8));
      // props.load(is);
    }

    return props;
  }

  private static URI toURI(URL url) throws IOException {
    try {
      return url.toURI();

    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI: " + url, e);
    }
  }

  private static List<String> listFilesInJar(URI uri, String resourcePath) throws IOException {
    // Create filesystem for JAR
    try (var fs = FileSystems.newFileSystem(uri, Map.of())) {
      var path = fs.getPath(resourcePath);
      try (var walk = Files.walk(path)) {
        return walk.filter(Files::isRegularFile)
            .map(Path::toString)
            .map(s -> s.startsWith("/") ? s.substring(1) : s) // Remove leading slash
            .toList();
      }
    }
  }

  private static List<String> listFilesInFileSystem(Path path, String resourcePath)
      throws IOException {
    try (var walk = Files.walk(path)) {
      return walk.filter(Files::isRegularFile)
          .map(p -> resourcePath + "/" + path.relativize(p))
          .toList();
    }
  }
}
