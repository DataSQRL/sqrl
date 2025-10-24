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
package com.datasqrl.loaders.schema;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.SchemaConversionResult;
import com.datasqrl.io.schema.TableSchemaFactory;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.util.FilenameAnalyzer;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SchemaLoaderImpl implements SchemaLoader {

  private final ResourceResolver resourceResolver;
  private final ErrorCollector errors;

  @Override
  public Optional<SchemaConversionResult> loadSchema(
      String tableName, String relativeSchemaFilePath, Map<String, String> tableProps) {
    var schemaFactoryMap = TableSchemaFactory.factoriesByExtension();
    var fileAnalyzer = FilenameAnalyzer.of(schemaFactoryMap.keySet());
    var fileComponentsOpt = fileAnalyzer.analyze(relativeSchemaFilePath);
    if (fileComponentsOpt.isEmpty()) {
      return Optional.empty();
    }

    var fileComponents = fileComponentsOpt.get();
    var pathNamePath = NamePath.parse(fileComponents.filename());
    var namePath =
        pathNamePath
            .popLast()
            .concat(pathNamePath.getLast().append(Name.system(fileComponents.getSuffix())));
    Optional<Path> path = resourceResolver.resolveFile(namePath);
    errors.checkFatal(
        path.isPresent(),
        "Could not load schema from: %s [via resolver %s]",
        namePath,
        resourceResolver);
    SchemaConversionResult schemaResult =
        schemaFactoryMap.get(fileComponents.extension()).convert(path.get(), tableProps, errors);
    return Optional.of(schemaResult);
  }
}
