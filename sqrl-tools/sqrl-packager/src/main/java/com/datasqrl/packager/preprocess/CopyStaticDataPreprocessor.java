package com.datasqrl.packager.preprocess;

import static com.datasqrl.actions.WriteDag.DATA_DIR;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.file.FilePath;
import com.google.auto.service.AutoService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@AutoService(Preprocessor.class)
/*
 * Copies jsonl and csv files (optionally with compression) to data directory to be added to flink
 */
@Slf4j
public class CopyStaticDataPreprocessor implements Preprocessor {

  public static final Set<String> DATA_FILE_EXTENSIONS = Set.of("jsonl","csv");
  public static final Set<String> COMPRESSION_EXENTESION = FilePath.COMPRESSION_EXTENSIONS;

  public static Pattern generateFileRegex() {
    return Pattern.compile(".*\\.(" +StringUtils.join(DATA_FILE_EXTENSIONS,"|")+ ")"
        + "(\\.("+ StringUtils.join(COMPRESSION_EXENTESION,"|") +"))?$");
  }

  @Override
  public Pattern getPattern() {
    return generateFileRegex();
  }

  @SneakyThrows
  @Override
  public void processFile(Path path, ProcessorContext processorContext, ErrorCollector errors) {
    Path dataDir = processorContext.getBuildDir().resolve(DATA_DIR);
    Files.createDirectories(dataDir);
    Path data = dataDir.resolve(path.getFileName());
    if (!Files.isRegularFile(data)) { //copy only if file does not already exist
      Files.copy(path, data);
    }
  }
}