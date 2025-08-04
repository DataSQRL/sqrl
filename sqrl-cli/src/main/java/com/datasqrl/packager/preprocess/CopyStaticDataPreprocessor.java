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

import com.datasqrl.packager.PreprocessorOrchestrator;
import com.datasqrl.util.FileCompression;
import com.datasqrl.util.FileCompression.CompressionIO;
import com.datasqrl.util.FilenameAnalyzer;
import com.datasqrl.util.FilenameAnalyzer.Components;
import com.google.common.io.ByteStreams;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/*
 * Copies jsonl and csv files (optionally with compression) to data directory to be added to flink
 */
@Slf4j
public class CopyStaticDataPreprocessor implements NewPreprocessor {

  public static final Set<String> DATA_FILE_EXTENSIONS = Set.of("jsonl", "csv");

  private static final FilenameAnalyzer DATA_PATTERN = FilenameAnalyzer.of(DATA_FILE_EXTENSIONS);

  @SneakyThrows
  @Override
  public void process(Path file, PreprocessorOrchestrator.Context context) {
    Optional<Components> fileComponents = DATA_PATTERN.analyze(file);
    if (fileComponents.isEmpty()) return;
    if (fileComponents.get().getExtension().equalsIgnoreCase("csv")) {
      Optional<CompressionIO> fileCompress =
          FileCompression.of(fileComponents.get().getCompression());
      if (fileCompress.isPresent()) {
        // Need to remove header row for CSV files since Flink does not support headers
        copyFileSkipFirstLine(file, context.createNewDataFile(file), fileCompress.get());
      } else {
        context
            .getErrorCollector()
            .warn(
                "Compression codex %s not supported. CSV file [%s] not copied.",
                fileComponents.get().getCompression(), file);
      }
    } else {
      context.copy2data(file);
    }
  }

  private void copyFileSkipFirstLine(Path from, Path to, CompressionIO fileCompress) {
    try (var in = fileCompress.decompress(new FileInputStream(from.toFile()));
        var out = fileCompress.compress(new FileOutputStream(to.toFile()))) {

      copyFileSkipFirstLine(in, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void copyFileSkipFirstLine(InputStream in, OutputStream out) throws IOException {
    var afterFirstLine = skipFirstLine(in);
    ByteStreams.copy(afterFirstLine, out);
  }

  private static InputStream skipFirstLine(InputStream rawIn) throws IOException {
    // Make sure we can mark and reset (peek the next byte safely)
    var in = (rawIn instanceof BufferedInputStream bis) ? bis : new BufferedInputStream(rawIn);

    while (true) {
      in.mark(1);
      var b = in.read();
      if (b == -1) {
        // End of stream, no more lines
        break;
      } else if (b == '\n') {
        // Found line break (Unix), done skipping
        break;
      } else if (b == '\r') {
        // Might be Windows-style \r\n or old Mac \r
        in.mark(1);
        var next = in.read();
        if (next != '\n' && next != -1) {
          // It's not \n, so reset => unread that byte
          in.reset();
        }
        // done skipping the first line
        break;
      }
      // Otherwise keep reading until we find line break or EOF
    }

    return in;
  }
}
