/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;


import com.google.common.base.Strings;
import com.google.common.io.Resources;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Contains a set of static methods for handling with files and folders.
 *
 * A lot of the methods are proxies around 3rd party libraries and don't have dedicated tests
 */
public class FileUtil {

  public static String getExtension(Path p) {
    return FilenameUtils.getExtension(p.getFileName().toString());
  }

  public static String removeExtension(Path p) {
    return FilenameUtils.removeExtension(p.getFileName().toString());
  }

  public static boolean isExtension(Path p, String extension) {
    return p.getFileName().toString().endsWith(extension);
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
    if (!folderName.startsWith(".")) folderName = "." + folderName;
    Path result = basePath.resolve(folderName);
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
    if (!extension.startsWith(".")) extension = "." + extension;
    return filename + extension;
  }

  public static String readResource(String resourceName) throws IOException {
    URL url = Resources.getResource(resourceName);
    return Resources.toString(url, StandardCharsets.UTF_8);
  }

  public static boolean isEmptyDirectory(Path dir) throws IOException {
    if (!Files.isDirectory(dir)) return true;
    try (Stream<Path> entries = Files.list(dir)) {
      return !entries.findFirst().isPresent();
    }
  }

  private static final int DELIMITER_CHAR = 46;

  public static Pair<String, String> separateExtension(Path path) {
    return separateExtension(path.getFileName().toString());
  }

  public static Pair<String, String> separateExtension(String fileName) {
    if (Strings.isNullOrEmpty(fileName)) {
      return null;
    }
    int offset = fileName.lastIndexOf(DELIMITER_CHAR);
    if (offset == -1) {
      return Pair.of(fileName, "");
    } else {
      return Pair.of(fileName.substring(0, offset).trim(), fileName.substring(offset + 1).trim());
    }
  }


  @SneakyThrows
  public static String readFile(URI uri) {
    URL url = uri.toURL();
    StringBuilder content = new StringBuilder();

    try (InputStream inputStream = url.openStream();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

      String line;
      while ((line = bufferedReader.readLine()) != null) {
        content.append(line).append(System.lineSeparator());
      }
    }

    return content.toString();
  }

  @SneakyThrows
  public static URI getParent(URI uri) {
    return new URI(uri.toString().substring(0, uri.toString().lastIndexOf("/") + 1));
  }
}
