/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;


import com.datasqrl.MainScriptImpl;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.parse.SqrlParserImpl;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.ScriptVisitor;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlCreateDefinition;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.StatementVisitor;

@AllArgsConstructor(onConstructor_=@Inject)
public class ImportExportAnalyzer implements
    StatementVisitor<Optional<NamePath>, Void>,
    ScriptVisitor<Set<NamePath>, Void> {
  private final ModuleLoader moduleLoader;
  private final SqlNameUtil nameUtil;
  private final ConnectorFactoryFactory connectorFactoryFactory;
  private final SqrlParser parser = new SqrlParserImpl();
  private final MainScriptImpl mainScript;
  @SneakyThrows
  public Set<NamePath> analyze(Path sqrlScript, ErrorCollector errors) {
    //TODO: use new parser
    return Set.of();
//    ScriptNode node;
//    try {
//      String scriptContent = mainScript.getContent();
//      errors = errors.withSchema("<schema>", scriptContent);
//      node = parser.parse(scriptContent);
//    } catch (Exception e) {
//      errors.warn("Could not compile SQRL script %s", sqrlScript);
//      throw errors.handle(e);
//    }
//
//    return node.accept(this, null);
  }

  @Override
  public Optional<NamePath> visit(SqrlCreateDefinition statement, Void context) {
    return Optional.empty();
  }

  @Override
  public Optional<NamePath> visit(SqrlImportDefinition node, Void context) {
    NamePath path = nameUtil.toNamePath(node.getImportPath().names);
    if (moduleLoader.getModule(path.popLast()).isPresent()) {
      return Optional.empty();
    }
    return Optional.of(path.popLast());
  }

  @Override
  public Optional<NamePath> visit(SqrlExportDefinition node, Void context) {
    NamePath sinkPath = nameUtil.toNamePath(node.getSinkPath().names);

    if (SystemBuiltInConnectors.forExport(sinkPath.popLast()).isPresent()) {
      return Optional.empty();
    }
    if (moduleLoader.getModule(sinkPath.popLast()).isPresent()) {
      return Optional.empty();
    }
    return Optional.of(sinkPath.popLast());
  }

  @Override
  public Optional<NamePath> visit(SqrlAssignment statement, Void context) {
    return Optional.empty();
  }

  @Override
  public Set<NamePath> visit(ScriptNode statement, Void context) {
    return statement.getStatements().stream()
        .map(s -> ((SqrlStatement)s).accept(this, null))
        .flatMap(Optional::stream)
        .collect(Collectors.toSet());
  }
}
