package com.datasqrl.packager.preprocess;

import static com.datasqrl.config.SqrlConstants.DATA_DIR;

import com.datasqrl.discovery.file.FilenameAnalyzer;
import com.datasqrl.discovery.file.FilenameAnalyzer.Components;
import com.datasqrl.error.ErrorCollector;
import com.google.common.io.ByteStreams;
import com.datasqrl.discovery.file.FileCompression;
import com.datasqrl.discovery.file.FileCompression.CompressionIO;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/*
 * Copies jsonl and csv files (optionally with compression) to data directory to be added to flink
 */
@Slf4j
public class CopyStaticDataPreprocessor implements Preprocessor {

  public static final Set<String> DATA_FILE_EXTENSIONS = Set.of("jsonl","csv");

  private static final FilenameAnalyzer DATA_PATTERN = FilenameAnalyzer.of(DATA_FILE_EXTENSIONS);
  private static final FilenameAnalyzer CSV_PATTERN = FilenameAnalyzer.of(Set.of("csv"));

  @Override
  public Pattern getPattern() {
    return DATA_PATTERN.getFilePattern();
  }

  @SneakyThrows
  @Override
  public void processFile(Path path, ProcessorContext processorContext, ErrorCollector errors) {
    Path dataDir = processorContext.getBuildDir().resolve(DATA_DIR);
    Files.createDirectories(dataDir);
    Path data = dataDir.resolve(path.getFileName());
    if (!Files.isRegularFile(data)) { //copy only if file does not already exist
      Optional<Components> match = CSV_PATTERN.analyze(path);
      if (match.isPresent()) {
        Optional<CompressionIO> fileCompress = FileCompression.of(match.get().getCompression());
        if (fileCompress.isPresent()) {
          //Need to remove header row for CSV files since Flink does not support headers
          copyFileSkipFirstLine(path, data, fileCompress.get());
        } else {
          errors.warn("Compression codex %s not supported. CSV file [%s] not copied.", match.get().getCompression(), path);
        }
      } else {
        Files.copy(path, data);
      }
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
    InputStream afterFirstLine = skipFirstLine(in);
    ByteStreams.copy(afterFirstLine, out);
  }

  private static InputStream skipFirstLine(InputStream rawIn) throws IOException {
    // Make sure we can mark and reset (peek the next byte safely)
    BufferedInputStream in = (rawIn instanceof BufferedInputStream) ? (BufferedInputStream) rawIn
        : new BufferedInputStream(rawIn);

    while (true) {
      in.mark(1);
      int b = in.read();
      if (b == -1) {
        // End of stream, no more lines
        break;
      } else if (b == '\n') {
        // Found line break (Unix), done skipping
        break;
      } else if (b == '\r') {
        // Might be Windows-style \r\n or old Mac \r
        in.mark(1);
        int next = in.read();
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