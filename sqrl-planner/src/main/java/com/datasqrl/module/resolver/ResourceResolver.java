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
package com.datasqrl.module.resolver;

import com.datasqrl.canonicalizer.NamePath;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public interface ResourceResolver {

  List<Path> loadPath(NamePath namePath);

  Optional<Path> resolveFile(NamePath namePath);

  Optional<Path> resolveFile(Path relativePath);

  static URL toURL(URI uri) {
    try {
      return uri.toURL();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static String getFileName(URI uri) {
    var pathSegments = uri.getPath().split("/");
    return pathSegments[pathSegments.length - 1];
  }

  static String getFileName(Path path) {
    return path.getFileName().toString();
  }
}
