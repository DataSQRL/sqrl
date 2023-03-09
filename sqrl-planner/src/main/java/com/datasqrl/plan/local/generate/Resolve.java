/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.generate;

import static com.datasqrl.error.PosToErrorPos.atPosition;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.*;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.plan.local.generate.StatementProcessor.ProcessorContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqrlStatement;

@Getter
@Slf4j
public class Resolve {

  private final NamespaceFactory namespaceFactory;
  private final ModuleLoader moduleLoader;
  DebuggerConfig debugger;
  NameCanonicalizer nameCanonicalizer;
  private final ErrorCollector errors;
  private final StatementProcessor statementProcessor;

  public Resolve(NamespaceFactory namespaceFactory, ModuleLoader moduleLoader, NameCanonicalizer nameCanonicalizer,
      ErrorCollector errors, StatementProcessor statementProcessor) {
    this.namespaceFactory = namespaceFactory;
    this.moduleLoader = moduleLoader;
    this.nameCanonicalizer = nameCanonicalizer;
    this.errors = errors;
    this.statementProcessor = statementProcessor;
  }

  public Namespace planDag(ScriptNode scriptNode) {
    ErrorCollector error = scriptNode.getScriptPath().isPresent()
        ? errors.withFile(scriptNode.getScriptPath().get(), scriptNode.getOriginalScript())
        : errors.withFile("test.sqrl", scriptNode.getOriginalScript());

    try {
      return planDagHelper(scriptNode, error);
    } catch (Exception e) {
      throw error.handle(e);
    }
  }

  protected Namespace planDagHelper(ScriptNode scriptNode, ErrorCollector error) {
    Namespace ns = namespaceFactory.createNamespace();

    scriptNode.getStatements()
        .stream().map(s -> (SqrlStatement) s)
        .forEach(s -> executeStatement(ns, s, error));

    return ns;
  }

  private void executeStatement(Namespace ns, SqrlStatement s, ErrorCollector error) {
    ErrorCollector errors = error
        .withLocation(atPosition(error, s.getParserPosition()));
    try {
      s.accept(statementProcessor,
          new ProcessorContext(ns, errors));
    } catch (Exception e) {
      throw errors.handle(e);
    }
  }
}