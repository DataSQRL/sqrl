package com.datasqrl.packager.preprocess;

import com.datasqrl.packager.PreprocessorOrchestrator;
import java.nio.file.Path;

public interface NewPreprocessor {

  void process(Path file, PreprocessorOrchestrator.Context context);
}
