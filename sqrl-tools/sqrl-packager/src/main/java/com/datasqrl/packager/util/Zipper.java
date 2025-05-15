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
package com.datasqrl.packager.util;

import com.datasqrl.util.FileUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ExcludeFileFilter;
import net.lingala.zip4j.model.ZipParameters;

public class Zipper {

  public static final String ZIP_EXTENSION = ".zip";

  public static void compress(Path zipFile, Path directory) throws IOException {
    Files.deleteIfExists(zipFile);
    ExcludeFileFilter excludeFilter = file -> file.getName().endsWith(ZIP_EXTENSION);
    var zipParameters = new ZipParameters();
    zipParameters.setExcludeFileFilter(excludeFilter);
    var zip = new ZipFile(zipFile.toFile());
    for (Path p : Files.list(directory).collect(Collectors.toList())) {
      if (Files.isRegularFile(p) && !FileUtil.isExtension(p, ZIP_EXTENSION)) {
        zip.addFile(p.toFile());
      } else if (Files.isDirectory(p)) {
        zip.addFolder(p.toFile(), zipParameters);
      }
    }
  }
}
