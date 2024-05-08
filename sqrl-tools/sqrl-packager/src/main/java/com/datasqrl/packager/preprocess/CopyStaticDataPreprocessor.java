package com.datasqrl.packager.preprocess;

import static com.datasqrl.actions.WriteDag.DATA_DIR;

import com.datasqrl.discovery.file.FilenameAnalyzer;
import com.datasqrl.discovery.file.FilenameAnalyzer.Components;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.discovery.file.FileCompression;
import com.datasqrl.discovery.file.FileCompression.CompressionIO;
import com.google.auto.service.AutoService;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
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
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileCompress.decompress(new FileInputStream(from.toFile()))));
        PrintWriter writer = new PrintWriter(fileCompress.compress(new FileOutputStream(to.toFile())))) {
      // Skip the first line
      reader.readLine();
      // Read from the second line until file end
      String line;
      while ((line = reader.readLine()) != null) {
        writer.println(line);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}