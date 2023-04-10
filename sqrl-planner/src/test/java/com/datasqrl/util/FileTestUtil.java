/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.Strings;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;

public class FileTestUtil {

  @SneakyThrows
  public static long countLinesInAllPartFiles(Path path) {
    AtomicLong lineCount = new AtomicLong(0);
    applyAllPartFileLines(path, lineStream -> lineCount.addAndGet(lineStream.count()));
    return lineCount.get();
  }

  @SneakyThrows
  public static List<String> collectAllPartFilesByLine(Path path) {
    List<String> result = new ArrayList<>();
    applyAllPartFileLines(path, lineStream -> lineStream.forEach(result::add));
    return result;
  }

  @SneakyThrows
  public static void applyAllPartFileLines(Path path, Consumer<Stream<String>> consumer) {
    for (File file : FileUtils.listFiles(path.toFile(), new RegexFileFilter("^part(.*?)"),
        DirectoryFileFilter.DIRECTORY)) {
      try (Stream<String> stream = Files.lines(file.toPath(), StandardCharsets.UTF_8)) {
        consumer.accept(stream);
      }
    }
  }

  @SneakyThrows
  public static Collection<Path> getAllFiles(Path dir) {
    try (Stream<Path> files = Files.walk(dir)) {
      return files.map(p -> dir.relativize(p)).collect(Collectors.toList());
    }
  }

  public static String getAllFilesAsString(Path dir) {
    Collection<String> files = getAllFiles(dir).stream().map(p -> p.toString())
        .filter(Predicate.not(Strings::isNullOrEmpty))
        .sorted().collect(Collectors.toList());
    return String.join("\n", files);
  }

  private static final ObjectMapper jsonMapper = SqrlObjectMapper.INSTANCE;
  private static final YAMLMapper yamlMapper = new YAMLMapper();

  static {
    jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
    yamlMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  @SneakyThrows
  public static <T> void writeJson(Path file, T object) {
    jsonMapper.writeValue(file.toFile(), object);
  }

  @SneakyThrows
  public static <T> String writeJson(T object) {
    return jsonMapper.writeValueAsString(object);
  }

  @SneakyThrows
  public static <T> void writeYaml(Path file, T object) {
    yamlMapper.writeValue(file.toFile(), object);
  }

  @SneakyThrows
  public static <T> String writeYaml(T object) {
    return yamlMapper.writeValueAsString(object);
  }

}
