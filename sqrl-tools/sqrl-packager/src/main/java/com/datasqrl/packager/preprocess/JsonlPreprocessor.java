package com.datasqrl.packager.preprocess;

import static com.datasqrl.actions.WriteDag.DATA_DIR;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@AutoService(Preprocessor.class)
/*
 * Copies json files to data directory to be added to flink
 */
@Slf4j
public class JsonlPreprocessor implements Preprocessor {

  public static final ObjectMapper mapper = SqrlObjectMapper.INSTANCE;

  @Override
  public Pattern getPattern() {
    return Pattern.compile(".*\\.jsonl$");
  }

  @SneakyThrows
  @Override
  public void processFile(Path path, ProcessorContext processorContext, ErrorCollector errors) {
    Path dataDir = processorContext.getBuildDir().resolve(DATA_DIR);
    Files.createDirectories(dataDir);
    Path data = dataDir.resolve(path.getFileName());
    if (!Files.isRegularFile(data)) {
      Files.copy(path, data);
    }
  }
}