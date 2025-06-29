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
package com.datasqrl.packager;

import com.datasqrl.MainScriptImpl;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlExportStatement;
import com.datasqrl.planner.parser.SqrlImportStatement;
import com.datasqrl.planner.parser.SqrlStatementParser;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ImportExportAnalyzer {
  private final ModuleLoader moduleLoader;
  private final SqlNameUtil nameUtil;
  private final ConnectorFactoryFactory connectorFactoryFactory;
  private SqrlStatementParser sqrlParser;
  private final MainScriptImpl mainScript;

  @SneakyThrows
  public Set<NamePath> analyze(String scriptContent, ErrorCollector errors) {
    return sqrlParser.parseScript(scriptContent, errors).stream()
        .map(ParsedObject::get)
        .flatMap(
            stmt -> {
              NamePath path;
              if (stmt instanceof SqrlImportStatement importStmt) {
                path = importStmt.getPackageIdentifier().get();
              } else if (stmt instanceof SqrlExportStatement exportStatement) {
                path = exportStatement.getPackageIdentifier().get();
                if (SystemBuiltInConnectors.forExport(path.popLast()).isPresent()) {
                  return Stream.of();
                }
              } else {
                return Stream.of();
              }
              NamePath pkgPath = path.popLast();
              if (moduleLoader.getModule(pkgPath).isEmpty()) {
                return Stream.of(pkgPath);
              }
              return Stream.of();
            })
        .collect(Collectors.toUnmodifiableSet());
  }
}
