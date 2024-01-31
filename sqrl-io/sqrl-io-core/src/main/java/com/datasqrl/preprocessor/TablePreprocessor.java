package com.datasqrl.preprocessor;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.util.NameUtil;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class TablePreprocessor implements Preprocessor {

  protected static boolean tableExists(Path basePath, String tableName) {
    Path tableFile = NameUtil.namepath2Path(basePath, NamePath.of(tableName + DataSource.TABLE_FILE_SUFFIX));
    return Files.isRegularFile(tableFile);
  }
}
