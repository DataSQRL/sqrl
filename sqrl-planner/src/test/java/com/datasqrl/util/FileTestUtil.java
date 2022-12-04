package com.datasqrl.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.Strings;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileTestUtil {

  @SneakyThrows
  public static int countLinesInAllPartFiles(Path path) {
    int lineCount = 0;
    for (File file : FileUtils.listFiles(path.toFile(), new RegexFileFilter("^part(.*?)"),
        DirectoryFileFilter.DIRECTORY)) {
      try (Stream<String> stream = Files.lines(file.toPath(), StandardCharsets.UTF_8)) {
        lineCount += stream.count();
      }
    }
    return lineCount;
  }

  ;

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

  private static final ObjectMapper jsonMapper = new ObjectMapper();
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
