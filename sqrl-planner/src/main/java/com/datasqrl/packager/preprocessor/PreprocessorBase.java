package com.datasqrl.packager.preprocessor;

import java.nio.file.Files;
import java.nio.file.Path;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.util.NameUtil;

public abstract class PreprocessorBase implements Preprocessor {

  protected static boolean tableExists(Path basePath, String tableName) {
    var tableFile = NameUtil.namepath2Path(basePath, NamePath.of(tableName + DataSource.TABLE_FILE_SUFFIX));
    return Files.isRegularFile(tableFile);
  }
}
