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

import com.datasqrl.canonicalizer.NamePath;
import java.nio.file.Path;
import java.util.List;

public class NameUtil {

  public static Path namepath2Path(Path basePath, NamePath path) {
    return createCanonicalPath(basePath, path.toStringList());
  }

  private static Path createCanonicalPath(Path basePath, List<String> pathList) {
    for (String path : pathList) {
      basePath = basePath.resolve(path.toLowerCase());
    }
    return basePath;
  }
}
