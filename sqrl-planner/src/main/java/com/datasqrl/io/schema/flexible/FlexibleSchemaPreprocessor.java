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
package com.datasqrl.io.schema.flexible;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocessor.PreprocessorBase;
import com.datasqrl.util.StringUtil;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@NoArgsConstructor
public class FlexibleSchemaPreprocessor extends PreprocessorBase {

  public static final String SCHEMA_YML_REGEX = "(.*)\\.schema\\.yml$";

  @Override
  public Pattern getPattern() {
    // Get a pattern for all files with the extension .schema.yml
    return Pattern.compile(SCHEMA_YML_REGEX);
  }

  @SneakyThrows
  @Override
  public void processFile(Path file, ProcessorContext processorContext, ErrorCollector errors) {
    Preconditions.checkArgument(Files.isRegularFile(file), "Not a regular file: %s", file);

    // Check if the directory contains a table json file
    String tablename =
        StringUtil.removeFromEnd(
            file.getFileName().toString(), FlexibleTableSchemaFactory.SCHEMA_EXTENSION);
    if (!tableExists(file.getParent(), tablename)) {
      errors.warn(
          "No table file [%s] for schema file [%s], hence schema is ignored", tablename, file);
      return;
    }

    processorContext.addDependency(file);
  }
}
