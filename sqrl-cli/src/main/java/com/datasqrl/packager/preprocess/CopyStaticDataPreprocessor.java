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

import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.FilenameAnalyzer;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

/**
 * Copies {@code .jsonl}, {@code .csv}, and {@code .avro} files (optionally with compression) to the
 * data folder unchanged, so they can be used by Flink.
 */
@Component
public class CopyStaticDataPreprocessor implements Preprocessor {

  private static final FilenameAnalyzer DATA_PATTERN = FilenameAnalyzer.of("jsonl", "csv", "avro");

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    if (DATA_PATTERN.analyze(file).isPresent()) {
      ctx.copyToData(file);
    }
  }
}
