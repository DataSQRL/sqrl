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

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocessor.Preprocessor;
import com.datasqrl.util.FileUtil;
import java.nio.file.Path;
import java.util.regex.Pattern;
import lombok.SneakyThrows;

/** Creates a package json based on given profiles and explicit package json references */
public class FlinkSqlPreprocessor implements Preprocessor {

  public static final Pattern DATASYSTEM_REGEX = Pattern.compile(".*" + FileUtil.toRegex(".sql"));

  @Override
  public Pattern getPattern() {
    return DATASYSTEM_REGEX;
  }

  @SneakyThrows
  @Override
  public void processFile(Path file, ProcessorContext processorContext, ErrorCollector errors) {

    processorContext.addDependency(file);
  }
}
