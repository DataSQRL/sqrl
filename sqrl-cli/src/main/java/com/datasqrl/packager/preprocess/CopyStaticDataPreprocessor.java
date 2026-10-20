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

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.FileCompression;
import com.datasqrl.util.FilenameAnalyzer;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

/**
 * Copies {@code .jsonl}, {@code .csv}, and {@code .avro} files (optionally with compression)
 * unchanged to the data folder, so they can be used by Flink. Flink reads every line of a CSV file
 * as data, so a CSV file that starts with a header row gets a warning.
 */
@Component
public class CopyStaticDataPreprocessor implements Preprocessor {

  private static final FilenameAnalyzer DATA_PATTERN = FilenameAnalyzer.of("jsonl", "csv", "avro");

  private static final List<String> CSV_DELIMITERS = List.of(",", ";", "\t");

  private static final int MAX_HEADER_LENGTH_IN_WARNING = 200;

  @SneakyThrows
  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    var fileComponents = DATA_PATTERN.analyze(file);
    if (fileComponents.isEmpty()) {
      return;
    }

    ctx.copyToData(file);

    var components = fileComponents.get();
    var compression = FileCompression.of(components.compression());
    if ("csv".equals(components.extension()) && compression.isPresent()) {
      var firstLine = readFirstLine(file, compression.get());
      if (isHeader(firstLine)) {
        warnHeader(file, firstLine, ctx.errorCollector());
      }
    }
  }

  /**
   * Returns whether the line looks like a header row: every field starts with a letter, as column
   * names do. A data row whose values all start with a letter looks like a header too.
   */
  static boolean isHeader(String line) {
    if (line == null) {
      return false;
    }
    var fields = line.startsWith("﻿") ? line.substring(1) : line;
    var delimiter =
        CSV_DELIMITERS.stream()
            .max(Comparator.comparingInt(d -> fields.split(Pattern.quote(d), -1).length))
            .get();
    return Arrays.stream(fields.split(Pattern.quote(delimiter), -1))
        .map(field -> unquote(field.strip()))
        .allMatch(field -> !field.isEmpty() && Character.isLetter(field.charAt(0)));
  }

  private static String unquote(String field) {
    return field.length() >= 2 && field.startsWith("\"") && field.endsWith("\"")
        ? field.substring(1, field.length() - 1).strip()
        : field;
  }

  private static void warnHeader(Path file, String header, ErrorCollector errors) {
    var shownHeader =
        header.length() > MAX_HEADER_LENGTH_IN_WARNING
            ? header.substring(0, MAX_HEADER_LENGTH_IN_WARNING) + "..."
            : header;
    errors.warn(
        "CSV file [%s] seems to start with a header row: %s. Flink reads every line of a CSV file"
            + " as data, so remove the header row from the file.",
        file, shownHeader);
  }

  private static String readFirstLine(Path file, FileCompression.CompressionIO compression)
      throws IOException {
    try (var reader =
        new BufferedReader(
            new InputStreamReader(
                compression.decompress(new FileInputStream(file.toFile())),
                StandardCharsets.UTF_8))) {
      return reader.readLine();
    }
  }
}
