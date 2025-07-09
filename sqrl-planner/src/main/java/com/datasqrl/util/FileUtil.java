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

import com.google.common.base.Strings;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Contains a set of static methods for handling with files and folders.
 *
 * <p>A lot of the methods are proxies around 3rd party libraries and don't have dedicated tests
 */
public class FileUtil {

  public static boolean isExtension(Path p, String extension) {
    return p.getFileName().toString().endsWith(extension);
  }

  public static Path combineWithRootIfRelative(Path rootDir, Path dir) {
    return dir.isAbsolute() ? dir : rootDir.resolve(dir);
  }

  /**
   * Creates a hidden directory in the provided basePath.
   *
   * @param basePath
   * @param folderName
   * @return The path to the hidden folder
   * @throws IOException
   */
  public static Path makeHiddenFolder(Path basePath, String folderName) throws IOException {
    if (!folderName.startsWith(".")) {
      folderName = "." + folderName;
    }
    var result = basePath.resolve(folderName);
    if (!Files.isDirectory(result)) {
      Files.createDirectories(result);
      if (SystemUtils.IS_OS_WINDOWS) {
        Files.setAttribute(result, "dos:hidden", Boolean.TRUE, LinkOption.NOFOLLOW_LINKS);
      }
    }
    return result;
  }

  public static void deleteDirectory(Path dir) throws IOException {
    if (Files.isDirectory(dir)) {
      FileUtils.deleteDirectory(dir.toFile());
    }
  }

  public static Path getUserRoot() {
    return FileUtils.getUserDirectory().toPath();
  }

  public static String addExtension(String filename, String extension) {
    if (!extension.startsWith(".")) {
      extension = "." + extension;
    }
    return filename + extension;
  }

  public static String readResource(String resourceName) throws IOException {
    var url = Resources.getResource(resourceName);
    return Resources.toString(url, StandardCharsets.UTF_8);
  }

  public static boolean isEmptyDirectory(Path dir) throws IOException {
    if (!Files.isDirectory(dir)) {
      return true;
    }
    try (var entries = Files.list(dir)) {
      return !entries.findFirst().isPresent();
    }
  }

  private static final int DELIMITER_CHAR = 46;

  public static Pair<String, String> separateExtension(Path path) {
    return separateExtension(path.getFileName().toString());
  }

  public static String getFileName(String path) {
    return Path.of(path).getFileName().toString();
  }

  public static Pair<String, String> separateExtension(String fileName) {
    if (Strings.isNullOrEmpty(fileName)) {
      return null;
    }
    var offset = fileName.lastIndexOf(DELIMITER_CHAR);
    if (offset == -1) {
      return Pair.of(fileName, "");
    } else {
      return Pair.of(fileName.substring(0, offset).trim(), fileName.substring(offset + 1).trim());
    }
  }

  public static String readFile(URI uri) {
    return BaseFileUtil.readFile(uri);
  }

  @SneakyThrows
  public static String readFile(Path path) {
    return Files.readString(path);
  }

  @SneakyThrows
  public static URI getParent(URI uri) {
    return new URI(uri.toString().substring(0, uri.toString().lastIndexOf("/") + 1));
  }

  public static String toRegex(String filename) {
    return filename.replace(".", "\\.");
  }
}
