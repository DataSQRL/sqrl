/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;


import com.datasqrl.MainScriptImpl;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkwrapper.parser.ParsedObject;
import com.datasqrl.flinkwrapper.parser.SqrlExportStatement;
import com.datasqrl.flinkwrapper.parser.SqrlImportStatement;
import com.datasqrl.flinkwrapper.parser.SqrlStatementParser;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor(onConstructor_=@Inject)
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
        .flatMap(stmt -> {
          NamePath path;
          if (stmt instanceof SqrlImportStatement) {
            SqrlImportStatement importStmt = (SqrlImportStatement) stmt;
            path = importStmt.getPackageIdentifier().get();
          } else if (stmt instanceof SqrlExportStatement) {
            SqrlExportStatement exportStatement = (SqrlExportStatement) stmt;
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
        }).collect(Collectors.toUnmodifiableSet());
  }

}
