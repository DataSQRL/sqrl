package com.datasqrl.packager.preprocessor;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.util.NameUtil;

import lombok.SneakyThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public abstract class PreprocessorBase implements Preprocessor {

	@SneakyThrows
  protected static boolean tableExists(Path basePath, String tableName) {
    String expectedFileName = NameUtil.namepath2Path(basePath, NamePath.of(tableName + DataSource.TABLE_FILE_SUFFIX)).getFileName().toString();

    try (Stream<Path> paths = Files.list(basePath)) {
      return paths.anyMatch(path ->
        Files.isRegularFile(path) &&
        path.getFileName().toString().equalsIgnoreCase(expectedFileName)
      );
    }
  }

}
