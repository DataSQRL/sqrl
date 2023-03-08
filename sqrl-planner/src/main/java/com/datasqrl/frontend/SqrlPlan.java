package com.datasqrl.frontend;

import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.DataSystemNsObject;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.calcite.table.QueryRelationalTable;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.NamespaceFactory;
import com.datasqrl.plan.local.generate.Resolve;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.local.generate.StatementProcessor;
import com.datasqrl.schema.SQRLTable;
import com.google.inject.Inject;
import java.util.Optional;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.tools.RelBuilder;

public class SqrlPlan extends SqrlParse {

  protected NamespaceFactory nsFactory;
  protected ModuleLoader moduleLoader;
  protected NameCanonicalizer nameCanonicalizer;
  protected StatementProcessor statementProcessor;
  protected SqrlQueryPlanner planner;
  private final DebuggerConfig debuggerConfig;

  @Inject
  public SqrlPlan(SqrlParser parser, ErrorCollector errors, NamespaceFactory nsFactory,
      ModuleLoader moduleLoader, NameCanonicalizer nameCanonicalizer,
      StatementProcessor statementProcessor, SqrlQueryPlanner planner, DebuggerConfig debuggerConfig) {
    super(parser, errors);
    this.nsFactory = nsFactory;
    this.moduleLoader = moduleLoader;
    this.nameCanonicalizer = nameCanonicalizer;
    this.statementProcessor = statementProcessor;
    this.planner = planner;
    this.debuggerConfig = debuggerConfig;
  }

  public Namespace plan(String script) {
    return plan(parse(script));
  }

  public Namespace plan(ScriptNode node) {
    Resolve resolve = new Resolve(this.nsFactory, this.moduleLoader,
        this.nameCanonicalizer, this.errors, this.statementProcessor);

    Namespace namespace = resolve.planDag(node);

    try {
      debug(this.planner, namespace, errors, this.moduleLoader, node, debuggerConfig);
    } catch (Exception e) {
      throw this.errors.handle(e);
    }
    return namespace;
  }

  private void debug(SqrlQueryPlanner planner, Namespace ns, ErrorCollector errors_,
      ModuleLoader moduleLoader, ScriptNode scriptNode, DebuggerConfig debugger) {
    ErrorCollector errors = errors_.withLocation(
        CompilerConfiguration.DebugConfiguration.getLocation());
    if (debugger.isEnabled()) {
      ns.getSchema().getAllTables().stream()
          .sorted((e1, e2) -> e1.getVt().getNameId().compareTo(e2.getVt().getNameId()))
          .forEach(tableEntry -> {
            VirtualRelationalTable vt = tableEntry.getVt();
            SQRLTable st = tableEntry;
            if (vt.isRoot() && debugger.debugTable(st.getName())) {
              QueryRelationalTable bt = vt.getRoot().getBase();
              if (bt.getExecution().isWrite()) {
                NamePath sinkPath = debugger.getSinkBasePath().concat(Name.system(vt.getNameId()));

                Optional<TableSink> sink = moduleLoader.getModule(sinkPath.popLast())
                    .flatMap(m -> m.getNamespaceObject(sinkPath.popLast().getLast()))
                    .map(s -> ((DataSystemNsObject) s).getTable())
                    .flatMap(dataSystem -> dataSystem.discoverSink(sinkPath.getLast(), errors))
                    .map(tblConfig ->
                        tblConfig.initializeSink(errors, sinkPath, Optional.empty()));

                errors.checkFatal(sink.isPresent(), ErrorCode.CANNOT_RESOLVE_TABLESINK,
                    "Cannot resolve table sink: %s", sinkPath);
                RelBuilder relBuilder = planner.createRelBuilder()
                    .scan(vt.getNameId());
                ns.addExport(new ResolvedExport(vt, relBuilder.build(), sink.get()));
              }
            }
          });
    }
  }
}
