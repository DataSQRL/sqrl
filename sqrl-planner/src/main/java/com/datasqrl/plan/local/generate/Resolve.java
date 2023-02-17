/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.generate;

import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.builtin.string.StdStringLibraryImpl;
import com.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.*;
import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.calcite.table.QueryRelationalTable;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.schema.SQRLTable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.tools.RelBuilder;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

@Getter
@Slf4j
public class Resolve {

  private final Path basePath;

  public Resolve(Path basePath) {
    this.basePath = basePath;
  }

  Namespace ns;
  ModuleLoader loader;

  public Namespace planDag(ScriptNode scriptNode, ErrorCollector errors,
      ResourceResolver resourceResolver,
      Session session) {
    session.setErrors(errors);
    try {
      return planDagHelper(scriptNode, errors, resourceResolver, session);
    } catch (Exception e) {
      throw errors.handle(e);
    }
  }

  protected Namespace planDagHelper(ScriptNode scriptNode, ErrorCollector errors,
      ResourceResolver resourceResolver,
      Session session) {
    Namespace ns = new Namespace(session);

    StandardLibraryLoader standardLibraryLoader = new StandardLibraryLoader(
        Map.of(
            Name.system("time").toNamePath(), new StdTimeLibraryImpl(),
            Name.system("string").toNamePath(), new StdStringLibraryImpl()
        ));

    ModuleLoader moduleLoader = new ModuleLoaderImpl(standardLibraryLoader,
        new ObjectLoaderImpl(resourceResolver, errors));

    SqrlStatementVisitor sqrlStatementVisitor = new SqrlStatementVisitor(
        SqrlStatementVisitor.SystemContext.builder()
            .errors(errors)
            .moduleLoader(moduleLoader)
            .nameCanonicalizer(NameCanonicalizer.SYSTEM)
            .build());

    scriptNode.getStatements()
        .stream().map(s -> (SqrlStatement) s)
        .forEach(s -> s.accept(sqrlStatementVisitor, ns));

    debug(ns, session, errors, moduleLoader, scriptNode);
    return ns;
  }

  private void debug(Namespace ns, Session session, ErrorCollector errors_,
      ModuleLoader moduleLoader, ScriptNode scriptNode) {
    DebuggerConfig debugger = ns.session.getDebugger();
    ErrorCollector errors = errors_.withLocation(
        CompilerConfiguration.DebugConfiguration.getLocation());
    if (debugger.isEnabled()) {
      session.getSchema().getAllTables().stream()
          .sorted((e1, e2) -> e1.getVt().getNameId().compareTo(e2.getVt().getNameId()))
          .forEach(tableEntry -> {
            VirtualRelationalTable vt = tableEntry.getVt();
            SQRLTable st = tableEntry;
            if (vt.isRoot() && debugger.debugTable(st.getName())) {
              QueryRelationalTable bt = vt.getRoot().getBase();
              if (bt.getExecution().isWrite()) {
                NamePath sinkPath = debugger.getSinkBasePath().concat(Name.system(vt.getNameId()));

                Optional<TableSink> sink = moduleLoader.getModule(sinkPath.popLast())
                    .flatMap(m->m.getNamespaceObject(sinkPath.popLast().getLast()))
                    .map(s -> ((DataSystemNsObject) s).getTable())
                    .flatMap(dataSystem -> dataSystem.discoverSink(sinkPath.getLast(), errors))
                    .map(tblConfig ->
                        tblConfig.initializeSink(errors, sinkPath, Optional.empty()));

                errors.checkFatal(sink.isPresent(), ErrorCode.CANNOT_RESOLVE_TABLESINK,
                    "Cannot resolve table sink: %s", sinkPath);
                RelBuilder relBuilder = ns.createRelBuilder()
                    .scan(vt.getNameId());
                ns.addExport(new ResolvedExport(vt, relBuilder.build(), sink.get()));
              }
            }
          });
    }
  }
}