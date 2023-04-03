/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.Value;

public interface TestGraphQLSchema {

  String getName();

  Path getSchemaPath();

  @SneakyThrows
  default String getSchema() {
    return Files.readString(getSchemaPath());
  }

  Map<String, String> getQueries();

  @Value
  class Directory implements TestGraphQLSchema {

    public static final String SCHEMA_FILE = "schema.graphqls";
    public static final String SCHEMA_FILE_EXTENSION = "graphqls";

    public static final String QUERY_FILE_SUFFIX = ".query.graphql";

    Path schemaDir;


    @Override
    public String getName() {
      return schemaDir.getFileName().toString();
    }

    @Override
    @SneakyThrows
    public Path getSchemaPath() {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(schemaDir,
          "*.{"+SCHEMA_FILE_EXTENSION+"}")) {
        for (Path entry : stream) {
          if (!Files.isDirectory(entry)) {
            return entry;
          }
        }
      }
      return null;
    }

    public static List<TestGraphQLSchema> of(Path... paths) {
      return Arrays.stream(paths).map(Directory::new).collect(Collectors.toList());
    }

    @Override
    @SneakyThrows
    public Map<String, String> getQueries() {
      Map<String, String> result = new LinkedHashMap<>();
      try (Stream<Path> files = Files.list(schemaDir).filter(Files::isRegularFile)
          .sorted((f1, f2) -> f1.getFileName().toString().compareTo(f2.getFileName().toString()))
      ) {
        files.forEach(f -> {
          String filename = f.getFileName().toString();
          if (filename.endsWith(QUERY_FILE_SUFFIX)) {
            try {
              result.put(StringUtil.removeFromEnd(filename, QUERY_FILE_SUFFIX),
                  Files.readString(f));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }
      return result;
    }
  }

}
